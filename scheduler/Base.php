<?php
/**
 * @file    MultiProcess_Scheduler.php
 * @author  刘重量
 * @date    2018/05/06 10:14:51
 * @brief   调度器抽象类
 * */
abstract class MultiProcess_Scheduler_Base extends MultiProcess_Base {

    const SUB_HOST_NAME = 'main-server';      //自定义服务器名（当取不到机器名的时候，取此值）
    const RETRY_COUNT = 3;          //重试次数限制

    protected $serverName = '';    //服务器名
    protected $workName = '';      //任务处理程序名

    protected $pid = 0;              //子进程pid
    protected $maxProcessPid = 0;  //主进程pid

    //该数组储存发包时间记录
    protected $arrPacketSendTime = array();


    /**
     * 非公开构造函数防止外部实例化.
     * @param callable $callHandler 回调函数
     * @param string   $namespace   命名空间
     */
    protected function __construct(callable $callHandler, $namespace) {
        $serverName = gethostname();
        $this->serverName = empty($serverName) ? self::SUB_HOST_NAME : $serverName;
        $handlerName = array(
            'namespace' => $namespace,
            'callback' => $callHandler,
        );
        $this->workName = serialize($handlerName);
        $this->jobStartTime = time();
        $this->init();
    }

    /**
     * 防止外部复制对象
     */
    final private function __clone() {}

    /**
     * 初始化函数
     */
    abstract public function init();

    /**
     * @param callable $handler  处理单个任务的回调函数
     * @param int      $forkNum  开启子进程数
     * @param int      $callType 调用类型
     * @return bool|mixed
     */
    public function exec(callable $handler, $forkNum, $callType) {
        $pids = array();         //储存子进程pid的数组
        $childProcessNum = 0;     //已开启的子进程数

        //至少创建了多次，$childProcessNum == 0才算不成功
        $upperRetryCount = self::RETRY_COUNT;
        while ($childProcessNum < $forkNum && $upperRetryCount > 0) {
            $pid = $this->forkChildProcessExec($handler,$upperRetryCount);
            if ($pid > 0) {
                $pids[] = $pid;
                ++$childProcessNum;
            }
        }
        if ($childProcessNum == 0) {
            throw new Exception('子进程派生不成功', self::ERROR_FORK_PROCESS_FAIL);
        }
        if ($callType == self::TYPE_CALL_WUNTRACED) {
            foreach ($pids as $pid) {
                pcntl_waitpid($pid, $status, WUNTRACED);
            }
        }
        return true;
    }

    /**
     * 获取作业上次的调度信息
     * @param  callable $callHandler 回调函数
     * @return mixed
     */
    abstract public function getLastDispatcherInfo(callable $callHandler);

    /**
     * 保存本次作业的调度信息
     * @param  callable $callHandler 回调函数
     * @param  array $arrPackets
     * @return mixed
     */
    abstract public function saveDispatcherInfo(callable $callHandler, array $arrPackets);

    /**
     * 加载本次执行的任务数组到调度器（产生以在redis中存储的任务id为索引，以原任务数组id为值的数组）
     * @param array $arrTasks    组成本次作业的任务列表数组
     * @param array $arrPackets  产生的任务包数组
     */
    public function loadArrTasks(array &$arrTasks, array &$arrPackets) {
        $arrTasks = array_unique($arrTasks, SORT_REGULAR);
        $this->arrPackets = array();
        foreach ($arrTasks as $taskId => $taskObj) {
            $packetId = self::KEYWORD_TASK . '_' . $this->getPacketId($taskObj);
            $this->arrPackets[$packetId] = $taskId;
        }
        $this->arrTasks = &$arrTasks;
        $arrPackets = $this->arrPackets;
    }

    /**
     * 设置执行业务的时间限制(同时也是子进程的存活期)
     * @param int $duration 时长(单位：秒)
     */
    public function setProcessDuration($duration) {
        $this->jobStartTime = time();
        $this->jobEndTime = $this->jobStartTime + $duration;
    }

    /**
     * 返回指定任务的在redis里存储的唯一id
     * @param string $task
     * @return bool|int
     */
    abstract protected function getPacketId($task);

    /**
     * 对待执行的任务包进行排序
     * @param int $sort 指定的排序方式
     * @return bool
     */
    final public function sortPackets($sort = self::SORT_NORMAL) {
        if ($sort == self::SORT_NORMAL) {
            return true;
        }
        $arrTaskCostTime = $this->getPacketHandlerTimeList();
        if (empty($arrTaskCostTime)) {
            return true;
        }
        $newTasks = array_diff_key($this->arrPackets, $arrTaskCostTime);
        $oldTasks = array_intersect_key($this->arrPackets,$arrTaskCostTime);

        if ($sort == self::SORT_ASC_BEFORE || $sort == self::SORT_ASC_AFTER) {
            asort($arrTaskCostTime,SORT_NUMERIC);
        } else {
            arsort($arrTaskCostTime,SORT_NUMERIC);
        }
        $oldTasks = array_merge($arrTaskCostTime, $oldTasks);
        if(empty($newTasks)) {
            $this->arrPackets = $oldTasks;
        }else {
            if ($sort == self::SORT_ASC_BEFORE || $sort == self::SORT_DESC_BEFORE) {
                $this->arrPackets = array_merge($newTasks, $oldTasks);
            } else {
                $this->arrPackets = array_merge($oldTasks, $newTasks);
            }
        }
        return true;
    }

    /**
     * 返回本类程序所开的所有子进程数
     * @return int
     */
    abstract public function getAllProcessNum();

    /**
     * 返回处理后等待分配子进程执行的任务数组（过滤掉已执行完成的任务和已分配子进程的任务）
     * @return array 未分配子进程执行进程的
     */
    abstract public function getWaitPackets();

    /**
     * 获取已执行结束的任务的模块(key)及执行的结束时间(value)组成的数组
     * @return array  (key:模块名称; value:执行结束的时间戳)
     */
    abstract public function getFinishPackets();

    /**
     * 获取处理完毕任务包的结果数组（key:模块名; value: 执行结果)
     * @return array
     */
    abstract public function getFinishPacketResults();

    /**
     * 返回任务包处理耗时的数组
     * @return array
     */
    abstract public function getPacketHandlerTimeList();

    /**
     * 返回处理后已分配子进程正在执行的任务数组列表（key:任务名）
     * @param array $arrPacketIds 指定的任务包id查询范围
     */
    public function getPacketAllocationList(array $arrPacketIds = array()) {
        $list = $this->getPacketsFromWorkPool($arrPacketIds);
        foreach ($list as $packetId => $v) {
            $pid = intval($v['pid']);
            $isRun = self::subProcessIsRun ($pid);

            //运行期内；非本机进程或者仍在运行中
            if (time() < $v['end_time'] && ($v['node'] != $this->serverName || $isRun)) {
                continue;
            }
            $packetStatus = self::PACKET_STATUS_WRONG;
            if ($v['end_time'] < time() && $isRun) {
                $res = posix_kill($pid, SIGKILL); //结束超时进程
                if (false === $res) { //如果终止进程失败
                    trigger_error('给子进程发送退出信号失败', E_USER_WARNING);
                }
            }

            //如果子进程的剩余运行时间小于执行失败的该任务包上次运行时间
            if ($v['end_time'] - $v['apply_time'] > $this->jobEndTime - time()) {
                $packetStatus = self::PACKET_STATUS_DELAY;
            }
            $this->savePacketStatus($packetId, $packetStatus,$v['apply_time']);
            $this->delPacketFromWorkPool($packetId);
            unset($list[$packetId]);
        }
        return $list;
    }

    /**
     * 申请一个任务包唯一分配给$pid对应的子进程执行(每进程每次只能申请到一个任务)
     * @return string
     */
    abstract public function applyTaskPacket();

    /**
     * 保存任务包的状态（完成时间、耗时、延迟执行)
     * @param string $packetName 任务名
     * @param int    $res      任务执行结果
     * @param int    $applyTime  申请（发包）时间
     */
    abstract public function savePacketStatus($packetName, $res, $applyTime = null);

    /**
     * 关闭子进程
     * @return bool
     */
    final public function closeChildProcess() {
        $this->delPidFromPool($this->pid);
        posix_kill($this->pid, SIGKILL);
        pcntl_waitpid($this->pid, $status, WUNTRACED);
    }


    /**
     * 产生一个分支进程执行作业，并把分进程的pid添加到进程信息表
     * @param  callable $handler  处理单个任务的回调函数
     * @param  int    $retryCount 重试次数
     * @return int
     */
    final protected function forkChildProcessExec($handler, &$retryCount) {
        do {
            $pid = pcntl_fork();
            if ($pid > 0) { //主进程得到是创建成功的子进程pid号，返回此pid
                return $pid;
            }
            if ($pid == 0) {
                $this->pid = posix_getpid();
                if (false === $this->pid) {//如果得不到自己的进程pid
                    exit(SIGKILL);
                } elseif(false === $this->addPidToPool()) { //得到进程pid,但添加入进程池失败
                    posix_kill($this->pid, SIGKILL);
                    pcntl_waitpid($this->pid,$status,WUNTRACED);
                }
                MultiProcess_Child::exec($this, $handler, $this->pid); //已包含结束语句；子进程执行完毕后会自动退出，无需在附加退出语句
            }
            usleep(rand(1, 1000000));
        }while(--$retryCount > 0);
        return false; // 派生子进程失败或者存储子进程的pid到进程信息表失败
    }

    /**
     * 返回来自已保存的任务分配表中记录信息
     * @param  array $arrPacketIds 指定的任务包id查询范围
     * @return array
     */
    abstract public function getPacketsFromWorkPool(array $arrPacketIds = array());

    /**
     * 从任务分配表中删除一个任务
     * @param string $packetName 被删除的任务包名（对应的表键）
     * @return bool
     */
    abstract public function delPacketFromWorkPool($packetName);

    /**
     * 清理本服务器保存在进程池中的僵尸进程和超时进程记录
     */
    final protected function cleanDefunctProcess() {
        $arrAllprocess = $this->getAllProcessInfo();
        foreach ($arrAllprocess as $pid => $endTime) {
            $isDel = true;//是否清除该进程
            if (is_numeric($pid) && $pid > 0 && 0 == pcntl_waitpid($pid, $status, WNOHANG) && false === pcntl_wifstopped($status)) {//该进程还在运行
                if (is_numeric($endTime) && $endTime > time()) {
                    $isDel = false;
                } elseif ($endTime < time()) { //超时
                    $isDel = posix_kill($pid, SIGKILL);
                    if (false === $isDel) {
                        trigger_error('failed to kill an sub process', E_USER_WARNING);
                    }
                }
            }
            if ($isDel) {
                $this->delPidFromPool($pid);
            }
        }
    }

    /**
     * 返回所在服务器（网络）节点内子进程信息表数组(key:进程pid;  vaule: 过期时间戳)
     * @return array
     */
    abstract protected function getAllProcessInfo();

    /**
     * 保存派生进程的pid到进程池
     * @return  bool
     */
    abstract protected function addPidToPool();

    /**
     * 从进程信息表中删除一个子进程的pid 记录
     * @param int $pid
     */
    abstract protected function delPidFromPool($pid);
}
