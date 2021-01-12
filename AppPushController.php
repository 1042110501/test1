<?php
/**
 * Created by PhpStorm.
 * User: PP
 * Date: 2017/12/19
 * Time: 10:51
 */

namespace console\controllers;

use common\components\Constant;
use common\components\Helper;
use common\components\Http;
use common\models\AppPush;
use common\models\BaseCountry;
use common\models\SignSetting;
use common\models\User;
use common\models\UserPushCount;
use common\models\UserPushMessage;
use common\models\UserPushToken;
use common\services\app\AppPushService;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Pool;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\ResponseInterface;
use yii\console\Controller;
use Yii;
use yii\db\Expression;

class AppPushController extends Controller
{
    const MAX_AUTO_PUSH_NUM = 3;

    /**
     * APP推送
     *
     * @edit pp 2020-03-26 修复bug，优化代码
     *
     * @edit pp 2020-03-23 改成分页执行，避免爆内存，另外限制一下单条进程数
     *
     * 定时每分钟执行一次
     * * * * * * nginx /usr/bin/php7.2 /data/www/ec_youmobi_com/yii app-push/auto-push > /dev/null 2>&1
     *
     * @param int $num 单条进程数
     *
     * @cmd php yii app-push/auto-push
     */
    public function actionAutoPush($num = 10)
    {
        if (Helper::countProcesses($this->getRoute()) > 1) {
            // pp 2020-03-26 准生产环境，Yii::end($msg);有问题，不输出里面的内容
            echo "process is running. failed" . PHP_EOL;
            exit;
        }

        $limit = $count = 100;
        $lastId = 0;
        //lastDay 统一用中国
        $lastDay = (new \DateTime('now', new \DateTimeZone('PRC')))->setTime(0, 0)->getTimestamp();
        $time = time();
        while ($limit == $count) {
            $count = 0;

            // pp 2020-03-26 原先代码，暂时保留
            /*$orMap = "send_way=".AppPush::SEND_WAY_TIMING." and status = ".AppPush::STATUS_ACTIVE." and last_day<".strtotime(date('Y-m-d', time()));
            $query1 = AppPush::find()->where(['status' => AppPush::STATUS_ACTIVE])->andWhere(['>', 'id', $lastId])
                ->andWhere(['<','failed_num', static::MAX_AUTO_PUSH_NUM])
                ->andWhere(['<=','push_time',time()])
                ->andwhere(['!=','target_type',AppPush::TARGET_TRADE])//不需要定时推送订单详情消息  做成同步触发推送
                ->andWhere(['push_status'=>[AppPush::PUSH_STATUS_WAIT,AppPush::PUSH_STATUS_ERROR],'send_way'=>[AppPush::SEND_WAY_DELAY,AppPush::SEND_WAY_TIME]])
                //->orWhere(['send_way'=>AppPush::SEND_WAY_TIMING,'status' => AppPush::STATUS_ACTIVE,])
                ->orWhere('id>' . $lastId . ' AND ' . $orMap);*/

            // pp 2020-03-26 改成where构造，不直接拼装，orWhere慎用
            $query = AppPush::find()
                ->where(['>', 'id', $lastId])
                ->andWhere(['status' => AppPush::STATUS_ACTIVE])
                ->andWhere([
                    'OR',
                    [
                        'AND',
                        ['send_way' => AppPush::SEND_WAY_TIMING],
                        ['<', 'last_day', $lastDay]
                    ],
                    [
                        'AND',
                        ['send_way' => AppPush::SEND_WAY_AUTO]
                    ],
                    [
                        'AND',
                        ['<', 'failed_num', static::MAX_AUTO_PUSH_NUM],
                        ['<=', 'push_time', $time],
                        ['!=', 'target_type', AppPush::APP_LINK_TYPE_TRADE],
                        [
                            'push_status' => [AppPush::PUSH_STATUS_WAIT, AppPush::PUSH_STATUS_ERROR],
                            'send_way' => [AppPush::SEND_WAY_DELAY, AppPush::SEND_WAY_TIME],
                        ]
                    ],
                ]);
            foreach ($query->orderBy('id ASC')->limit($limit)
                         ->asArray()->all() as $appPush) {
                $count++;
                $lastId = $appPush['id'];

                $country = BaseCountry::findOne(['id' => $appPush['country_id']]);
                $timezone = $country['timezone'] ?: 'PRC';
                $dateTime = (new \DateTime('now', new \DateTimeZone($timezone)));
                $countryNowTime = $dateTime->getTimestamp();
                //每天定时执行的特殊处理下
                if ($appPush['send_way'] == AppPush::SEND_WAY_TIMING) {
                    $pointTime = $appPush['point_time'] ? explode(':', $appPush['point_time']) : [];
                    $pushTime = $dateTime->setTime($pointTime[0] ?? 0, $pointTime[1] ?? 0, $pointTime[2] ?? 0)->getTimestamp();
                    $isPush = $appPush['last_day'] != $lastDay && ($countryNowTime >= $pushTime && $countryNowTime <= $pushTime + $appPush['ttl_time']);
                    if (!$isPush) {
                        continue;
                    }
                } elseif ($appPush['send_way'] == AppPush::SEND_WAY_AUTO) {
                    if (empty($appPush['push_time_setting'])) {
                        continue;
                    }
                    //判断时间，过滤不需要发送
                    $pushTimeSetting = json_decode($appPush['push_time_setting'], true);
                    if ($pushTimeSetting) {
                        $pushTimeSetting['start_time'] = explode(':', $pushTimeSetting['start_time']);
                        $pushTimeSetting['start_time'] =
                            $dateTime->setTime($pushTimeSetting['start_time'][0], $pushTimeSetting['start_time'][1], $pushTimeSetting['start_time'][2])->getTimestamp();
                        $pushTimeSetting['end_time'] = explode(':', $pushTimeSetting['end_time']);
                        $pushTimeSetting['end_time'] =
                            $dateTime->setTime($pushTimeSetting['end_time'][0], $pushTimeSetting['end_time'][1], $pushTimeSetting['end_time'][2])->getTimestamp();
                    }
                    $appPush['push_setting'] = !empty($appPush['push_setting']) ? json_decode($appPush['push_setting'], true) : [];
                    if (((empty($appPush['push_setting']) || (empty($appPush['push_setting']['setting_days']) && empty($appPush['push_setting']['setting_not_receive']) && empty($appPush['push_setting']['setting_receive'])))
                            && $appPush['last_day'] == $lastDay) ||
                        ($countryNowTime < $pushTimeSetting['start_time']) ||
                        ($pushTimeSetting['end_time'] > $lastDay && $pushTimeSetting['end_time'] < $countryNowTime)
                    ) {
                        continue;
                    }
                }

                echo "run {$lastId}" . PHP_EOL;

                $pushId = $appPush['id'];

                // pp 2020-03-23 限制一下进程数，不能一次性全部抛出去
                while (Helper::countProcesses('singleAppPush') >= $num) {
                    echo "waiting running." . PHP_EOL;
                    usleep(1000 * 5000);
                }

                // pp 2020-03-25 直接调用，不使用后台执行的方式
                // pp 2020-03-26 问题不是出在这里，还是改成后台脚本执行，加快发送速度
                //$this->actionSingle($pushId, 'auto');

                $cmd = Yii::$app->params['phpBin'] . " " . Yii::$app->getBasePath() . "/../yii app-push/single {$pushId} auto singleAppPush";
                echo "execInBackground: " . $cmd . PHP_EOL;
                Helper::execInBackground($cmd);
            }
            echo "deal {$count}, {$lastId}" . PHP_EOL;
        }

        echo "SUCCESS" . PHP_EOL;
    }

    /**
     * 添加每天定时发送
     *
     * @edit pp 2020-03-13 把推送api请求单独抽离出去，共用
     *
     * @edit tom 2019-11-25
     *
     * @cmd php yii app-push/single $pushId
     *
     * @param $pushId
     * @param string $type
     * @return bool
     */
    public function actionSingle($pushId, $type = 'handle')
    {
        // pp 2020-03-26 若已有pushId在执行，则无需再次执行，避免重复发送
        if (Helper::countProcesses($this->getRoute() . ' ' . $pushId . ' ') > 1) {
            // pp 2020-03-26 准生产环境，Yii::end($msg);有问题，不输出里面的内容
            echo "process is running. failed" . PHP_EOL;
            exit;
        }

        $appPush = AppPush::findOne(['id' => $pushId]);

        if ($appPush['status'] != AppPush::STATUS_ACTIVE) {
            return false;
        }

        $country = BaseCountry::findOne(['id' => $appPush['country_id']]);

        $timezone = $country['timezone'] ?: 'PRC';
        $dateTime = (new \DateTime('now', new \DateTimeZone($timezone)));
        $time = $dateTime->getTimestamp();
        //daytime 统一用中国
        $dayTime = (new \DateTime('now', new \DateTimeZone('PRC')))->setTime(0, 0)->getTimestamp();
        switch ($appPush['send_way']) {
            case AppPush::SEND_WAY_DELAY://定时发送
            case AppPush::SEND_WAY_TIME://实时发送
                if (in_array($appPush['push_status'], [AppPush::PUSH_STATUS_SUCCESS, AppPush::PUSH_STATUS_PUSHING])) {
                    return false;
                }
                //定时消息提前一分钟左右推送
                if ($appPush['send_way'] == AppPush::SEND_WAY_TIME && ($appPush['planned_push_time'] - $time) > 60) {
                    return false;
                }
                //自动超过三次不在推送
                if ($type == 'auto' && $appPush['failed_num'] >= static::MAX_AUTO_PUSH_NUM) {
                    return false;
                }
                break;
            case AppPush::SEND_WAY_TIMING://@add tom 每天定时
                $pointTime = $appPush['point_time'] ? explode(':', $appPush['point_time']) : [];
                $pushTime = $dateTime->setTime($pointTime[0] ?? 0, $pointTime[1] ?? 0, $pointTime[2] ?? 0)->getTimestamp();
                $isPush = $appPush['last_day'] != $dayTime && ($time >= $pushTime && $time <= $pushTime + $appPush['ttl_time']);
                if (!$isPush) {
                    return false;
                }
                break;
            case AppPush::SEND_WAY_AUTO: //@add tom 自动触发
                if (empty($appPush['push_time_setting'])) {
                    return false;
                }
                //判断时间，过滤不需要发送
                $pushTimeSetting = json_decode($appPush['push_time_setting'], true);
                if ($pushTimeSetting) {
                    $pushTimeSetting['start_time'] = explode(':', $pushTimeSetting['start_time']);
                    $pushTimeSetting['start_time'] =
                        $dateTime->setTime($pushTimeSetting['start_time'][0], $pushTimeSetting['start_time'][1], $pushTimeSetting['start_time'][2])->getTimestamp();
                    $pushTimeSetting['end_time'] = explode(':', $pushTimeSetting['end_time']);
                    $pushTimeSetting['end_time'] =
                        $dateTime->setTime($pushTimeSetting['end_time'][0], $pushTimeSetting['end_time'][1], $pushTimeSetting['end_time'][2])->getTimestamp();
                }
                if ((empty($appPush['push_setting']) && $appPush['last_day'] == $dayTime) ||
                    ($time < $pushTimeSetting['start_time']) ||
                    ($pushTimeSetting['end_time'] > $dayTime && $pushTimeSetting['end_time'] < $time)
                ) {
                    return false;
                }
                break;
            default:
                return false;
        }

        //如果是签到活动
        if ($appPush['target_type'] == AppPush::APP_LINK_TYPE_SIGN) {
            $signSetting = SignSetting::find()
                ->where(['country_id' => $appPush['country_id'], 'status' => Constant::STATUS_ACTIVE])->orderBy(['id' => SORT_DESC])->asArray()->limit(1)->one();
            if (!$signSetting) {
                return false;
            }
        }

        //变更推送状态
        $appPush->setAttributes([
            'push_status' => AppPush::PUSH_STATUS_PUSHING,
        ], false);
        $appPush->save(false);

        //推送类型跟appType对应起来
        $appType = AppPush::$targetPushMaps;

        //$type = (string)$appType[$appPush['target_type']];
        //choi 2020-9-2 目标类型改为统一app_link_type数据  所以不再转换 直接取值
        $type = (string)$appPush['target_type'];
        $target = strval($appPush['target_type'] == AppPush::TARGET_NONE ? $appPush['id'] : $appPush['target']);

        // V3.11 | 9866
        $sourceAdRef = "";
        if ($appPush['source'] == AppPush::PUSH_SOURCE_HANDLE_ADD) {
            $sourceAdRef = "https://firebase.com/openfrompush?utrack=1915&type={$type}&target={$target}";
            $newLinkId = AppPushService::getAppPushNewLinkId($appPush['id']);
            if (!empty($newLinkId)) {
                $sourceAdRef .= "&new_link_id=" . $newLinkId;
            }
        }
//        $sourceAdRef = $appPush['source'] == AppPush::PUSH_SOURCE_HANDLE_ADD ?
//            "https://firebase.com/openfrompush?utrack=1915&type={$type}&target={$target}" : "";

        // 签到的类型要更改文案
        if ($appPush['target_type'] == Constant::TYPE_SIGN) {
            $content = Yii::t('api', $appPush['content'], [], 'en');
        } else {
            $content = $appPush['content'];
        }

        if (!empty($appPush['thumbnail'])) {
            //带图片信息
            $pushData = [
                'message' => [
                    'token' => '',
                    'notification' => ['title' => $appPush['title'], 'body' => $content],
                    'data' => [
                        'type' => $type,
                        'target' => $target,
                        'thumbnail' => $appPush['thumbnail'],
                        'source_ad_ref' => $sourceAdRef,
                    ],
                ],
            ];
        } else {
            $pushData = [
                'message' => [
                    'token' => '',
                    'notification' => ['title' => $appPush['title'], 'body' => $content],
                    'data' => [
                        'type' => $type,
                        'target' => $target,
                        'source_ad_ref' => $sourceAdRef,
                    ],
                ],
            ];
        }
        $pushData = AppPushService::thumbnailPushDeal($pushData);
        $result = $this->pushToUsers($appPush, $pushData);
        $appPush->updateAttributes([
            'push_status' => $result['failed'] ? AppPush::PUSH_STATUS_ERROR : AppPush::PUSH_STATUS_SUCCESS,
            'failed_num' => $appPush['failed_num'] + ($result['failed'] ? 1 : 0),
            'success_total' => $appPush['success_total'] + intval($result['fulfilled-200']),
            'failed_total' => $appPush['failed_total'] + intval($result['failed']),
            'failed_reason' => "Push result: " . json_encode($result) . "; " . json_encode($pushData),
            'last_day' => $dayTime,
            'real_push_time' => time(),
        ]);

        echo "running push success: {$result['success']} failed: {$result['failed']} and 404Err: {$result['error']}" . PHP_EOL;
        return true;
    }

    /**
     * 并发推送
     * @param AppPush $appPush
     * @param $pushData
     * @return array
     */
    private function pushToUsers(AppPush $appPush, $pushData)
    {
        $returnData = ['success' => 0, 'failed' => 0, 'error' => 0];
        // 生成请求
        $pushTokens = [];
        $requests = function () use ($appPush, $pushData, &$pushTokens, &$returnData) {
            foreach (AppPushService::getPushTokens($appPush) as $index => $pushToken) {
                // android 4月17日22点前忽略推送
                if ($pushToken['platform'] == UserPushToken::PLATFORM_ANDROID && $pushToken['create_time'] < 1555509600) {
                    continue;
                }

                //如果该token已推送该消息则跳过【每天定时的消息和自动触发不写入消息记录】
                if ($appPush['send_way'] != AppPush::SEND_WAY_TIMING && $appPush['send_way'] != AppPush::SEND_WAY_AUTO
                    && UserPushMessage::find()->where(['push_id' => $appPush['id'], 'token_id' => $pushToken['id']])->count()) {
                    $returnData['success']++;
                    $returnData['sent']++;
                    continue;
                }

                $pushData['message']['token'] = $pushToken['token'];
                $pushTokens[$index] = $pushToken;
                yield $index => new Request('POST', AppPushService::$postUrls[$pushToken['platform']], [
                    'Authorization' => 'Bearer ' . AppPushService::getFirebaseAccessToken($pushToken['platform']),
                ], json_encode($pushData));
            }
        };
        // 并发推送
        $client = new Client(['timeout' => 5, 'allow_redirects' => false]);
        $pool = new Pool($client, $requests() ?: [], [
            'concurrency' => 10,
            'fulfilled' => function (ResponseInterface $response, $index) use (&$pushTokens, $appPush, $pushData, &$returnData) {
                $pushToken = $pushTokens[$index];
                if ($response->getStatusCode() == 401) {
                    AppPushService::getFirebaseAccessToken($pushToken['platform'], true);
                    $pushData['message']['token'] = $pushToken['token'];
                    $result = AppPushService::push($pushData, $pushToken['platform']);
                } else {
                    $result = ['response' => json_decode($response->getBody()->getContents(), true), 'http_code' => $response->getStatusCode()];
                }
                $returnData[$this->handlePushMessage($appPush, $pushToken, $result['http_code'], $result['response'])]++;
                $returnData['fulfilled-' . $result['http_code']]++;
                unset($pushTokens[$index]);
            },
            'rejected' => function (RequestException $e, $index) use (&$pushTokens, $appPush, $pushData, &$returnData) {
                $pushToken = $pushTokens[$index];
                $response = $e->getResponse();
                $statusCode = $response ? $response->getStatusCode() : 0;
                $returnData[$this->handlePushMessage($appPush, $pushToken, $statusCode, [])]++;
                $returnData['rejected-' . $statusCode]++;
                unset($pushTokens[$index]);
            },
        ]);
        ($pool->promise())->wait();

        return $returnData;
    }

    /**
     * 处理推送返回消息
     * @param AppPush $appPush
     * @param array $pushToken
     * @param $httpCode
     * @param $response
     * @return string 返回处理类型
     */
    private function handlePushMessage(AppPush $appPush, array $pushToken, $httpCode, $response): string
    {
        if ($httpCode == 404) {
            UserPushToken::updateAll([
                'status' => UserPushToken::STATUS_DELETED
            ], ['id' => $pushToken['id']]);
            return 'error';
        }

        if (empty($response)) {
            return 'failed';
        }

        if (!in_array($appPush['send_way'], [AppPush::SEND_WAY_TIMING, AppPush::SEND_WAY_AUTO])) { //每天定时和自动触发的不写入消息表
            $userPushMessage = new UserPushMessage();
            $userPushMessage->setAttributes([
                'push_id' => $appPush['id'],
                'user_id' => $pushToken['user_id'],
                'token_id' => $pushToken['id'],
                'create_time' => time(),
                'push_time' => time(),
                'is_read' => UserPushMessage::IS_READ_N,
                'status' => UserPushMessage::STATUS_NORMAL,
            ], false);
            try {
                $userPushMessage->save(false);
            } catch (\Exception $e) {
            }
        }
        //@edit tom 2020-6-10 自动触发的信息写入推送日志表
        if ($appPush['send_way'] == AppPush::SEND_WAY_AUTO) {
            //统一用中国
            $dayTime = (new \DateTime('now', new \DateTimeZone('PRC')))->setTime(0, 0)->getTimestamp();
            try {
                $userPushCount = UserPushCount::findOne([
                    'push_id' => $appPush['id'],
                    'push_token_id' => $pushToken['id'],
                    'day_time' => $dayTime
                ]);
                if (!$userPushCount) {
                    $userPushCount = new UserPushCount();
                    $userPushCount->setAttributes([
                        'push_id' => $appPush['id'],
                        'push_token_id' => $pushToken['id'],
                        'day_time' => $dayTime,
                        'total' => 1,
                        'create_time' => time(),
                        'update_time' => time()
                    ], false);
                } else {
                    $userPushCount->setAttributes([
                        'total' => new Expression('total+1'),
                        'update_time' => time(),
                    ], false);
                }

                $userPushCount->save(false);
            } catch (\Exception $e) {
                Yii::error($e->getMessage());
            }
        }
        return 'success';
    }

    /**
     * 初始化字段 app_push.push_time字段
     * jiajin - 2019/05/07 - 跑一次
     */
    public function actionInitPushTime()
    {
        foreach (AppPush::find()->andWhere(['!=', 'status', AppPush::STATUS_DELETED])->asArray()->all() as $row) {
            $countryTimeZone = BaseCountry::find()->select('timezone')->where(['id' => intval($row['country_id'])])
                ->limit(1)->scalar();

            $date = $row['planned_push_time'] ? date('Y-m-d H:i:s', $row['planned_push_time']) : '';

            if ($countryTimeZone) {
                date_default_timezone_set($countryTimeZone); //对应国家时区
            }

            $pushTime = $date ? strtotime($date) : 0;

            date_default_timezone_set('PRC'); //还原时区

            AppPush::findOne(['id' => $row['id']])->updateAttributes([
                'push_time' => $pushTime,
            ]);

            echo "{$row['id']}--$pushTime\n";
        }
        echo "ok" . "\n";
    }
}