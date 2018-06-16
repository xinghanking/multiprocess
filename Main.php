<?php
/**
 * @file   Multiprocess.php
 * @author liuzhongliang(13439694341@qq.com)
 * @date   2018/05/06 10:14:51
 * @brief  主进程类
 **/
class MultiProcess_Main extends MultiProcess_Base {

    private $jobName = '';   //作业名
    private $nameSpace = __NAMESPACE__; //命名空间
    /**
     * @var \MultiProcess_Scheduler_Base 任务调度名或者一个任务调度器对象
     */
    private $objScheduler = self::DEF_SCHEDULER;      //任务调度器名或者对象

    private $callHandler  = null;                     //用来执行单个任务的回调函数
    private $callType     = self::TYPE_CALL_WNOHANG;  //设定的本次执行业务的调用方式
    private $packetSort   = self::SORT_ASC_BEFORE;    //对待执行任务排序方式，取值：0-不排序;
    private $forkNum      = self::DEF_FORK_NUM;       //本次业务处理所开的最大进程数
    private $runTime      = 86400;                    //异步调用时为每个子进程可被结束运行的最大时长;同步调用时为本次作业的最大时长，单位:秒;

    private $arrLastQueryResult = array(); //上次查询结果

    /**
     * 构造函数(私有，单例)
     * @param callable        $callHandler 回调函数
     * @param array|string    $arrInit     配置项数组
     * @return mixed
     */
    private function __construct(callable $callHandler, $arrInit = array()) {
        try {
            if (false === self::is_CallHandler($callHandler)) {
                throw new Exception('所传任务处理回调函数不可用', self::ERROR_WRONG_PARAM);
            }
            $this->callHandler = $callHandler;
            $this->jobName = serialize($callHandler);
            if(!empty($arrInit) && is_array($arrInit)) {
                $this->init($arrInit);
            }
        } catch (Exception $e) {
            self::recordError($e->getCode(), $e->getMessage());
            return false;
        }
    }

    /** 重建对象
     * @param callable $callHandler 回调函数
     */
    protected static function rebuildInstance(callable $callHandler) {
        $jobName = serialize($callHandler);
        self::$arr_Instances[__NAMESPACE__][$jobName] = new self($callHandler);
        self::$obj_Instance = self::$arr_Instances[__NAMESPACE__][$jobName];
    }

    /**
     * 设置本次作业的各配置项的值
     * @param  array $arrInit 用来给配置项赋值的数组
     * @return bool           如果参数值存在非法值，赋值失败就报错返回false
     */
    public function init(array $arrInit) {
        try {
            if (!empty($this->callHandler) && !empty($arrInit['callHandler'])) {
                throw new Exception('已完成初始化的对象属性callHandler不允许重新赋值;若有必要请另建对象', self::ERROR_WRONG_PARAM);
            }
            $arrCheckParams = array_intersect_key(self::$arrParamsFormat, $arrInit);//筛选出允许被赋值的配置项数组
            foreach ($arrCheckParams as $arg => $checkParam) {
                if (is_string($arrInit[$arg])) {//如果参数值是字符串，去除一下首尾空格
                    $arrInit[$arg] = trim($arrInit[$arg]);
                }
                if ($arrInit[$arg] !== '') {
                    if (false === self::$checkParam($arrInit[$arg])) {//如果参数值存在非法值，赋值失败就报错返回
                        throw new Exception('所传参数配置项 ' . $arg . ' 的值不符合要求', self::ERROR_WRONG_PARAM);
                    } else {
                        $this->$arg = $arrInit[$arg];
                    }
                }
            }
        } catch (Exception $e) {
            $this->recordError($e->getCode(), $e->getMessage());
            return false;
        }
    }

    /**
     * 返回项目当前时间所有应用本类的程序在当前服务器所开启的子进程数
     * @return int
     */
    public function getProcessNum() {
        $num = $this->objScheduler->getAllProcessNum();
        return $num;
    }

    /**
     * 返回项目使用本类的应用程序当前允许范围的在当前服务器可新开启的子进程数
     * @return int
     */
    public function getCanForkNum() {
        $freeNum = self::FORK_LIMIT - $this->getProcessNum();
        return $freeNum;
    }

    /**
     * 开始多进程处理的执行函数
     * @return bool|\MultiProcess_Main
     */
    public function execute() {
        try {
            //如果是静态调用
            if (empty($this) || !($this instanceof  self)) {
                $args = func_get_args();
                $argNum = count($args);
                if (empty($args) || $argNum <1) {
                    return false;
                }
                $obj = self::getInstance($args[0], __NAMESPACE__);
                if (!($obj instanceof self)) {
                    return false;
                }
                if ($argNum > 1) {
                    $args = array_slice($args,1);
                    $initKeys = array_keys(self::$arrParamsFormat);
                    $initKeys = array_slice($initKeys, 1, $argNum-1);
                    $arrInit  = array_combine($initKeys, $args);
                    $obj->init($arrInit);
                }
                return $obj->execute();
            }

            //检查执行的条件是否已具备
            $this->checkPreconditions();
            //格式化任务包数组;过滤掉已被相同程序处理中的任务包;得到本次需开启的进程数
            $this->preExecute();

            //开始执行
            $res = $this->objScheduler->exec($this->callHandler, $this->forkNum, $this->callType);
            //执行完毕
            if ($res) {
                //如果是异步调用,保存一下本次的调度信息
                if ($this->callType == self::TYPE_CALL_WNOHANG) {
                    $this->objScheduler->saveDispatcherInfo($this->callHandler, $this->arrPackets);
                }
                return $this;
            } 
            return false;
        } catch (Exception $e) {
            $this->recordError($e->getCode(), $e->getMessage());
            if(self::ERROR_NOT_NEED_EXEC == $e->getCode()) {
                return $this;
            }
            return false;
        }
    }

    /**
     * 返回当前业务（任务集）执行的状态详情
     * @param callable $callHandler 执行任务的函数
     * @param string   $namespace   命名空间
     * @return array|bool
     */
    public function getJobExecDetails($callHandler = null, $namespace = __NAMESPACE__) {
        try {
            /**
             * 对静态调用和传参了的非直接调用对象的情况处理
             */
            if (empty($this)) {//如果是静态调用
                $obj = null;
                if (is_callable($callHandler) && self::is_callHandler($callHandler)) {
                    $obj = self::getInstance($callHandler);
                }
                if (empty($callHandler) && $namespace == __NAMESPACE__) {
                    $obj = self::$obj_Instance;
                }
                if($obj instanceof self) {
                    return $obj->getJobExecDetails();
                }
                return false;
            } elseif (!empty($callHandler) || $namespace!= __NAMESPACE__) {
                if(!empty($callHandler) && self::is_callHandler($callHandler) && ($callHandler != $this->callHandler || $namespace!= __NAMESPACE__)) {
                    $obj = self::getInstance($callHandler);
                    if ($obj instanceof self) {
                        return $obj->getJobExecDetails();
                    }
                }
                return false;
            }

            /**
             * 对象正式开始执行查询
             */
            if (!empty($this->arrLastQueryResult) && !in_array($this->arrLastQueryResult['status'], array(
                    self::JOB_STATUS_STILL_WAIT,
                    self::JOB_STATUS_PROCESSING,
                    self::JOB_STATUS_PART_FAIL,
                ))) {//如果上次查询结果，作业的状态是已经结束了的状态，那么直接返回上次的查询结果
                return $this->arrLastQueryResult; //上次查询结果作业状态是已停止运行，直接返回
            }

            //任务包数组为空，说明是对象重建后查询，判断是否有上次的调度信息恢复
            if (empty($this->arrPackets)) {
                $objScheduler = $this->objScheduler;
                $this->objScheduler = $objScheduler::getInstance($this->callHandler);
                if (!($this->objScheduler instanceof MultiProcess_Scheduler_Base)) {
                    return false;
                }
                $this->arrPackets = $this->objScheduler->getLastDispatcherInfo($this->callHandler);
            }

            //初始化查询结果
            $result = array(
                'job'    => $this->jobName, //作业名
                'namespace' => $this->nameSpace, //命名空间
                'start_time' => date('Y-m-d H:i:s', $this->objScheduler->jobStartTime), //作业开始时间
                'status' => self::JOB_STATUS_STILL_WAIT, //作业当前的状态值
                'desc'   => self::$arrJobExecDesc[self::JOB_STATUS_STILL_WAIT], //作业当前的状态描述
                'list'   => array(), //详细列表
            );
            foreach (self::$arrPacketStatusDesc as $status => $desc) {
                $result['list'][$status] = array(
                    'status' => $status, //任务包状态名
                    'desc'   => $desc,
                    'total'  => 0,
                    'ids'    => array(), //所属成员id列表
                );
            }

            /**
             * 如果是任务数组都未设置的对象，直接返回空结果
             */
            if (empty($this->arrPackets)) {
                return $result;
            }

            $packetResults = array();

            //如果是异步调用，获取正在等待执行的任务组和正在执行的任务组信息
            if ($this->callType == self::TYPE_CALL_WNOHANG) {
                /**
                 * 获取等待执行的任务数组
                 */
                $waitPackets = $this->objScheduler->getWaitPackets();//从调度器中获得当前正在等待分配子进程执行任务的任务包成员数组
                if (!empty($waitPackets) && is_array($waitPackets)) {
                    $packetResults = array_fill_keys(array_keys($waitPackets), self::PACKET_STATUS_WAIT);
                }

                /**
                 * 获取正在执行的任务包id等信息
                 */
                $processPackets = $this->objScheduler->getPacketAllocationList(array_keys($this->arrPackets));
                if (!empty($processPackets) && is_array($processPackets)) {
                    $processPacketResults = array_fill_keys(array_keys($processPackets), self::PACKET_STATUS_PROCESS);
                    $packetResults = array_merge($packetResults, $processPacketResults);
                }
            }

            /**
             * 获取已完成的任务包id等信息
             */
            $itemFinishResults = $this->objScheduler->getFinishPacketResults();
            if (!empty($itemFinishResults) && is_array($itemFinishResults)) {
                $packetResults = array_merge($packetResults, $itemFinishResults);
            }

            /**
             * 获取丢失处理的任务包数组（如果有的话）
             */
            $missPackets = array_diff_key($this->arrPackets, $packetResults);
            if (!empty($missPackets)) {
                $missPacketsResults = array_fill_keys(array_keys($missPackets), self::PACKET_STATUS_WRONG);
                $packetResults = array_merge($packetResults, $missPacketsResults);
            }

            $total = array_count_values($packetResults);
            foreach ($packetResults as $packetId => $status) {
                $result['list'][$status]['total'] = $total[$status];
                $result['list'][$status]['ids'][] = $this->arrPackets[$packetId];
            }

            /**
             * 得出当前作业的状态
             */
            $packetNum = count($this->arrPackets);
            if ($result['list'][self::PACKET_STATUS_WAIT]['total'] == $packetNum) { //如果等待执行数等于总任务数
                return $result;
            }

            $packetNum -= $result['list'][self::PACKET_STATUS_WAIT]['total'];//去掉等待包数的剩余总包数

            if ($result['list'][self::PACKET_STATUS_PROCESS]['total'] > 0) {//如果现在还有正在执行的任务包
                $status = ($result['list'][self::PACKET_STATUS_PROCESS]['total'] + $result['list'][self::PACKET_STATUS_COMPLETE]['total'] == $packetNum) ? self::JOB_STATUS_PROCESSING : self::JOB_STATUS_PART_FAIL;
            } elseif ($result['list'][self::PACKET_STATUS_WAIT]['total'] == 0) {//如果等待包数也为0，说明作业已结束
                $status = $result['list'][self::PACKET_STATUS_COMPLETE] == $packetNum ? self::JOB_STATUS_COMPLETE : self::JOB_STATUS_END;
            } else {//等待包数大于0，正在执行的包数为0，说明子进程全部结束
                $status = self::JOB_STATUS_WRONG_STOP;
                if ($this->objScheduler->jobEndTime < time()) {
                    $finshPacketEndTimeList = $this->objScheduler->getFinishPackets();
                    if (empty($finshPacketEndTimeList) && is_array($finshPacketEndTimeList)) {
                        throw new Exception('从调度器中获取任务完成表失败', self::ERROR_QUERY_FROM_SCHEDULER);
                    }
                    $maxEndTime = max($finshPacketEndTimeList);
                    if ($maxEndTime > $this->objScheduler->jobEndTime) {
                        $status = self::JOB_STATUS_TIMEOUT_END;
                    }
                }
            }
            $result['status'] = $status;
            $result['desc'] = self::$arrJobExecDesc[$status];
            $this->arrLastQueryResult = $result;
            return $result;
        } catch (Exception $e) {
            $this->recordError($e->getCode(), $e->getMessage());
            return false;
        }
    }

    /**
     * 返回最后一次的错误信息
     * @return array
     */
    public function getError() {
        return self::$error;
    }

    /**
     * 检查执行的前提条件——必须的配置项是否已赋值可用
     */
    private function checkPreconditions() {
        if (empty($this->callHandler) || empty($this->arrTasks)) {
            throw new Exception('缺少可用的回调执行函数传入或执行的对象任务数组', self::ERROR_MISS_PARAM);
        }
        if (is_subclass_of($this->objScheduler, 'MultiProcess_Scheduler_Base')) {
            $objScheduler = &$this->objScheduler;
            $this->objScheduler = $objScheduler::getInstance($this->callHandler);
        }
        if (!($this->objScheduler instanceof MultiProcess_Scheduler_Base)) {
            throw new Exception('调度器不可用', self::ERROR_WRONG_SCHEDULER);
        }
        $canForkNum = $this->getCanForkNum();
        if ($canForkNum == 0) {
            throw new Exception('项目开启子进程数已经达到上限', self::ERROR_OVER_FORK_UPPER);
        }
        $this->forkNum = min($this->forkNum, $canForkNum);
        if ($this->forkNum == 0) {
            $this->forkNum = min(self::DEF_FORK_NUM, $canForkNum);
        }
    }

    /**
     * 格式化任务包数组；过滤掉已被相同程序处理中的任务包；得到本次开启的进程数
     * @throws Exception
     */
    private function preExecute() {

        //加载本次要执行的任务进调度器
        $this->arrPackets = $this->objScheduler->loadArrTasks($this->arrTasks);

        //设置子进程的执行时长（运行时限; 单位：秒）
        $this->objScheduler->setProcessDuration($this->runTime);

        //如果是异步调用,则预存下本次调度信息
        if ($this->callType == self::TYPE_CALL_WNOHANG) {
            $this->objScheduler->saveDispatcherInfo($this->callHandler, $this->arrPackets);
        }

        //返回调度器处理后将本次将要执行的任务数(过滤掉已有其它程序执行的相同成员任务)
        $waitTasks = $this->objScheduler->getWaitPackets();
        if (empty($waitTasks)) {
            if ($this->callType == MultiProcess_Base::TYPE_CALL_WNOHANG) {//如果是异步执行，直接返回
                throw new Exception('所有任务已有其它相同程序执行，本次无需执行', self::ERROR_NOT_NEED_EXEC);
            }
        } else {
            //对将要执行的任务排序
            $this->objScheduler->sortPackets($this->packetSort);
        }
        //如果等待执行的任务包数小于预设的开启进程数，则修改开启进程数为等待执行的任务包数
        $waitTaskNum = count($waitTasks);
        $this->forkNum = $waitTaskNum < $this->forkNum ? $waitTaskNum : $this->forkNum;
    }

    /**
     * 魔术方法，当一个对象被外部当成一个函数调用时
     * @param array         $arrTasks     由多个等到被回调函数处理的任务对象 组成的任务组 数组
     * @param int           $processNum   开启的多进程数
     * @param int           $callType     调用方式类型，1.异步调用非阻塞;  2.同步调用.阻塞
     * @param int           $limiTime     限时
     * @param int           $packetSort   执行顺序
     * @param string|object $subScheduler 调度器
     * @return bool|object
     */
    public function __invoke(array $arrTasks, $processNum = 5, $limiTime = 86400, $callType = self::TYPE_CALL_WNOHANG, $packetSort = self::SORT_NORMAL, $subScheduler = self::DEF_SCHEDULER) {
        try{
            $arrInit = array(
                'arrTasks'   => $arrTasks,
                'forkNum'    => $processNum,
                'callType'   => $callType,
                'packetSort' => $packetSort,
                'limtTime'   => $limiTime,
                'scheduler'  => $subScheduler,
            );
            $this->init($arrInit);
            $res = $this->execute();
            return $res;
        } catch (Exception $e) {
            $this->recordError($e->getCode(), $e->getMessage());
            return false;
        }
    }

    /**
     * 魔术方法，支持静态调用exec和init 函数
     * @param  string $name      内部函数名
     * @param  array  $arguments 参数数组
     * @return bool|mixed
     */
    public static function __callStatic($name, array $arguments) {
        try{
            if ($name == 'exec' && count($arguments) > 1 && is_callable($arguments[0])) {
                $obj = self::getInstance($arguments[0]);
                if ($obj instanceof self) {
                    unset($arguments[0]);
                    $res = call_user_func_array($obj, $arguments);
                    return $res;
                }
            }
            if ($name == 'init' && !empty($arguments[0]) && is_array($arguments[0])) {
                $arrInit = &$arguments[0];
                if (!empty($arrInit['callHandler'])) {
                    $obj = self::getInstance($arrInit['callHandler']);
                } else {
                    $obj = self::$obj_Instance;
                }
                if ($obj instanceof self) {
                    return $obj->init($arrInit);
                }
            } elseif ($name == 'getJobExecDetails' && (empty($arguments[0]) || is_string($arguments[0]))) {
                $obj = empty($arguments[0]) ? self::$obj_Instance : self::getInstance($arguments[0]);
                if ($obj instanceof self) {
                    return $obj->getJobExecDetails($arguments[0]);
                }
            } elseif ($name == 'getError') {
                return self::$error;
            }
            return false;
        } catch (Exception $e) {
            if(self::$obj_Instance instanceof  self) {
                self::$obj_Instance->recordError($e->getCode(), $e->getMessage());
            }
            return false;
        }
    }
}
