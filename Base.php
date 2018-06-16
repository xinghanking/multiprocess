<?php

/**
 * Class MultiProcess_Base
 * @author 刘重量(13439694341@qq.com)
 * @date   2018/05/06 10:14:51
 * @brief  多进程执行类的基类
 **/
abstract class MultiProcess_Base {
    /*
     * 项目常量定义
     */
    const APP_NAME             = 'LeiDa';                        //项目名
    const FORK_LIMIT           = 800;                            //项目内本类所有应用在单个服务器系统中同时存在的所开最大进程数
    const KEYWORD_PROCESS      = 'process';                      //子进程信息表名称关键词标识
    const KEYWORD_TASK         = 'task';                         //任务关键词标识
    const CALSS_MAIN           = 'MultiProcess_Main';            //主调类名
    const CLASS_SCHEDULER_BASE = 'MultiProcess_Scheduler_Base';  //调度器基类名

    /*
     * 调用类型常量定义
     */
    const TYPE_CALL_WNOHANG   = 1;     //非阻塞调用类型，有子进程创建成功并开始执行任务就可以返回
    const TYPE_CALL_WUNTRACED = 2;     //阻塞调用类型，待所有子进程执行完任务才返回

    /*
     * 任务处理优先级排序选项常量定义
     */
    const SORT_NORMAL      = 0;    //原序，不做顺序调整
    const SORT_ASC_BEFORE  = 1;    //按照上次执行耗时从短到长升序排列, 新任务排到前面
    const SORT_ASC_AFTER   = 2;    //按照上次执行耗时从短到长升序排列, 新任务排到后面
    const SORT_DESC_BEFORE = 3;    //按照生词执行耗时从长到短降序排列, 新任务排到前面
    const SORT_DESC_AFTER  = 4;    //按照生词执行耗时从长到短降序排列, 新任务排到后面

    /*
     * 默认值常量
     */
    const DEF_SCHEDULER = 'MultiProcess_Scheduler_By_Redis';    //默认使用的调度器
    const DEF_FORK_NUM  = 40;                                   //默认本次调用设置的子进程数

    /*
     * 作业完成状态值
     */
    const JOB_STATUS_STILL_WAIT  = 0;
    const JOB_STATUS_PROCESSING  = 1;
    const JOB_STATUS_PART_FAIL   = 2;
    const JOB_STATUS_WRONG_STOP  = 3;
    const JOB_STATUS_TIMEOUT_END = 4;
    const JOB_STATUS_END         = 5;
    const JOB_STATUS_COMPLETE    = 6;

    /*
     * 任务包完成状态值
     */
    const PACKET_STATUS_WAIT     = 'wait';      // 任务包由于子进程超时一直未执行或者执行中因为子进程超时而未完成
    const PACKET_STATUS_PROCESS  = 'handler';   // 任务包状态名，处理中
    const PACKET_STATUS_DELAY    = 'delay';     // 任务包状态名，推迟处理；由于上次执行时长大于子进程剩余的生存期，估计这次也会超时，被调度器推迟到下次执行
    const PACKET_STATUS_WRONG    = 'wrong';     // 任务包状态名  处理时发生致命错误，导致子进程退出;或者其它原因导致无记录
    const PACKET_STATUS_FAILED   = 'failed';    // 数组该成员执行完成，返回结果为false
    const PACKET_STATUS_COMPLETE = 'complete';  // 数组该成员执行完成，无返回值或返回值非false

    /*
     * 错误码选项
     */
    const ERROR_FREE                 = 0;     //无错误
    const ERROR_WRONG_PARAM          = 1;     //错误的参数值
    const ERROR_MISS_PARAM           = 2;     //必需参数没有传
    const ERROR_WRONG_SCHEDULER      = 3;     //调度器不可用
    const ERROR_OVER_FORK_UPPER      = 4;     //进程数已达到上限
    const ERROR_NOT_NEED_EXEC        = 5;     //所有任务已有其它相同程序执行，本次无需执行
    const ERROR_FORK_PROCESS_FAIL    = 6;     //创建子进程失败
    const ERROR_QUERY_FROM_SCHEDULER = 7;     //从调度器中查询数据时发生错误
    const ERROR_REDIS_WRONG          = 8;     //redis故障
    const ERROR_SYS_WRONG            = 9;     //系统故障


    /**
     * @var array 公用变量设置
     */
    protected $arrTasks   = array(); //本次执行的任务数组
    protected $arrPackets = array(); //本次执行的任务包数组（以任务在调度器里的id为key,对应任务id为value ）

    //实例化数组
    protected static $arr_Instances = array();
    protected static $obj_Instance  = null;    //当前实例;


    //存储最近一次的错误记录
    protected static $error = array(
        'no'  => self::ERROR_FREE, //错误码
        'msg' => '运行良好，无错误',
    );

    /**
     * @var array 允许赋值使用的调用类型范围数组
     */
    protected static $arrCallType = array(
        self::TYPE_CALL_WUNTRACED,
        self::TYPE_CALL_WNOHANG,
    );

    /**
     * @var array 任务包优先级排序选项数组
     */
    protected static $arrSortType = array(
        self::SORT_NORMAL,
        self::SORT_ASC_BEFORE,
        self::SORT_ASC_AFTER,
        self::SORT_DESC_BEFORE,
        self::SORT_DESC_AFTER,
    );

    /**
     * @var array 作业（任务集）完成状态选项数组
     */
    public static $arrJobExecDesc = array(
        self::JOB_STATUS_STILL_WAIT  => '未执行或等待执行',
        self::JOB_STATUS_PROCESSING  => '执行中,运行良好，尚未有错误发生',
        self::JOB_STATUS_PART_FAIL   => '执行中，有部分模块执行错误或失败',
        self::JOB_STATUS_WRONG_STOP  => '执行过程中发生错误，已停止运行',
        self::JOB_STATUS_TIMEOUT_END => '因为执行超时被终止执行，未完成',
        self::JOB_STATUS_END         => '执行结束;有返回false的模块或者执行时发生错误的模块',
        self::JOB_STATUS_COMPLETE    => '执行结束，无返回值false或执行出错的模块',
    );

    /**
     * @var array 任务包状态名说明选项数组
     */
    public static $arrPacketStatusDesc = array(
        self::PACKET_STATUS_WAIT     => '等待',
        self::PACKET_STATUS_PROCESS  => '处理中',
        self::PACKET_STATUS_DELAY    => '延迟处理, 触发条件：上次执行超时，本次执行子进程剩余时间小于其上次执行时长',
        self::PACKET_STATUS_WRONG    => '执行中发生致命错误，导致子进程退出或无记录',
        self::PACKET_STATUS_FAILED   => '执行完后有返回值且返回值为失败',
        self::PACKET_STATUS_COMPLETE => '执行完后无返回值或返回值非false',
    );

    /**
     * @var array 参数校验函数数组
     * 数组key部分为各配置项的名称对应赋值数组的参数名
     * 数组value部分为对应的处理该参数值的类函数名
     */
    protected static $arrParamsFormat = array(
        'callHandler'   => 'is_callHandler',
        'arrTasks'      => 'is_arrTasks',
        'forkNum'       => 'is_forkNum',
        'runTime'       => 'is_runTime',
        'callType'      => 'is_callType',
        'packetSort'    => 'is_packetSort',
        'scheduler'     => 'is_scheduler',
    );

    /**
     * 获取一个对象
     * @param callable $callHandler 执行任务的回调函数
     * @return object
     */
    public static function getInstance(callable $callHandler) {
        $jobName = serialize($callHandler);
        $className = get_called_class();
        if(isset(static::$arr_Instances[__NAMESPACE__][$jobName]) && static::$arr_Instances[__NAMESPACE__][$jobName] instanceof $className) {//如果已存在实例
            static::$obj_Instance = static::$arr_Instances[__NAMESPACE__][$jobName];
        } else{
            static::rebuildInstance($callHandler);
        }
        return static::$obj_Instance;
    }

    /**
     * 重建对象
     * @param callable $callHandler 执行任务的回调函数
     */
    protected static function rebuildInstance(callable $callHandler){
        $jobName = serialize($callHandler);
        $className = get_called_class();
        self::$arr_Instances[__NAMESPACE__][$jobName] = new $className($callHandler);
        self::$obj_Instance = self::$arr_Instances[__NAMESPACE__][$jobName];
    }

    /**
     * 保存最后一次出错信息
     * @param int    $no  错误码
     * @param string $msg 说明信息
     */
    protected function recordError($no, $msg) {
        self::$error = array(
            'no' => $no,
            'msg'=> $msg,
        );
    }

    /**
     * 检查所传调度器是否可用
     * @param string|object $objScheduler 调度器的类名或者一个对象
     * @return bool
     */
    protected static function is_scheduler($objScheduler) {
        if ((is_string($objScheduler) || is_object($objScheduler)) && is_subclass_of($objScheduler, 'MultiProcess_Scheduler_Base')) {
            return true;
        }
        return false;
    }

    /**
     * 检查所传调用格式参数的值是否在设置范围内，给调用格式callType属性赋值
     * @param int $callType 本次调用处理业务的方式类型: 1.异步调用，非阻塞，有子进程存活并开始处理任务就返回成功;2.同步调用，阻塞，等待所有任务被执行完毕，返回成功
     * @return bool
     */
    protected static function is_callType($callType) {
        return in_array($callType, self::$arrCallType);
    }

    /**
     * 检查给任务排序所传参数值是否在可用范围内，给类属性packetSort赋值
     * @param int $packetSort
     * @return bool
     */
    protected static function is_packetSort($packetSort) {
        return in_array($packetSort, self::$arrSortType);
    }

    /**
     * 检查执行任务的回调函数是否可用，为类属性callHandler赋值
     * @param callable $callHandler 执行单个任务的回调函数
     * @return bool
     */
    protected static function is_callHandler(callable $callHandler) {
        return ((is_string($callHandler) && function_exists($callHandler)) || (is_array($callHandler) && method_exists($callHandler[0], $callHandler[1])));
    }

    /**
     * 检测所传值是否合法，设置本次运行所能开启的最大子进程数
     * @param int $forkNum 本次执行所能生成的最大子进程数
     * @return bool
     */
    protected static function is_forkNum($forkNum) {
        return (is_numeric($forkNum) && $forkNum > 0);
    }

    /**
     * 检查所传值是否合法，格式化参数，设置子进程最大运行时间的值
     * @param int $limitTime 子进程运行的最大时间限制
     * @return bool
     */
    protected static function is_runTime($limitTime) {
        return (is_numeric($limitTime) && $limitTime > 0);
    }

    /**
     * 设置本次执行任务列表数组的值
     * @param array $arrTaskPackets 组成执行业务的任务列表数组
     * @return bool
     */
    protected static function is_arrTasks(array $arrTaskPackets) {
        return (is_array($arrTaskPackets) && count($arrTaskPackets) > 0);
    }

    /**
     * 检测子进程是否在运行中没有退出(因为本程序子进程都交由系统托管，无需主进程用wait_pid读取起运行状态，所以可以用此方法判断子进程是否退出)
     * @param int $pid 子进程pid
     * @return bool
     */
    protected static function subProcessIsRun ($pid) {
        $gid = posix_getpgid($pid);  //取不到gid证明pid已不存在
        return $gid === false ? false : true;
    }
}
