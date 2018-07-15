<?php
/**
 * @file    MultiProcess_Scheduler.php
 * @author  刘重量(13439694341@qq.com)
 * @date    2018/05/06 10:14:51
 * @brief   调度器抽象类
 * */
abstract class MultiProcess_Scheduler_Base extends MultiProcess_Base {

    const SUB_HOST_NAME   = 'main-server';    //自定义服务器名（当取不到机器名的时候，取此值）
    const RETRY_COUNT     = 3;                  //重试次数限制

    protected $serverName = '';                 //服务器名
    protected $workName   = '';                 //任务处理程序名

    protected $pid        = 0;                  //进程id
    protected $pids       = array();            //子进程pid组成的数组

    public $jobStartTime  = 0;                  //执行作业（任务集）的开始时间戳
    public $jobEndTime    = 0;                  //执行作业（任务集）的限期完成时间戳

    //该数组储存发包时间记录
    protected $arrPacketSendTime = array();


    /**
     * 非公开构造函数防止外部实例化.
     * @param callable $callHandler 回调函数
     */
    protected function __construct(callable $callHandler) {
        $serverName       = gethostname();
        $this->serverName = empty($serverName) ? self::SUB_HOST_NAME : $serverName;

        $scope = static::getScope();
        $handlerName = array(
            'namespace' => $scope,
            'callback'  => $callHandler,
        );

        $this->workName     = serialize($handlerName);
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
        $childProcessNum = 0;     //已开启的子进程数
        //至少创建了多次，$childProcessNum == 0才算不成功
        $upperRetryCount = self::RETRY_COUNT;
        pcntl_signal(SIGCHLD, SIG_IGN);  //主进程忽略SIGCHLD信号，这样子进程在主进程推出后会被交由系统托管，可防止产生僵尸进程
        while ($childProcessNum < $forkNum && $upperRetryCount > 0) {
            $pid = $this->forkChildProcessExec($handler,$upperRetryCount);
            if ($pid > 0) {
                $this->pids[] = $pid;
                ++$childProcessNum;
            }
        }
        if ($childProcessNum == 0 && empty($this->pids)) {
            trigger_error('Failed to fork process!', E_USER_WARNING);
            throw new Exception('子进程创建不成功', self::ERROR_FORK_PROCESS_FAIL);
        }
        if ($callType == self::TYPE_CALL_WUNTRACED) {//如果是同步调用
            while(count($this->pids) > 0) {
                //清理已结束运行的子进程pid
                foreach ($this->pids as $k => $pid) {
                    if(false === self::subProcessIsRun($pid)) {
                        unset($this->pids[$k]);
                    }
                }

                //如果所有的子进程都结束运行了
                if(empty($this->pids)) {
                    break;
                }

                //有子进程还在运行，且运行结束时间未到
                if(time() < $this->jobEndTime) { 
                    sleep(1);
                    continue;
                }

                //本次作业运行限时已用完，那就发信号中止现在还在运行的子进程
                foreach($this->pids as $pid) {
                    if (false === posix_kill($pid, SIGKILL)) {
                        trigger_error('Failed to send sinal to a child process, pid:' . $pid, E_USER_WARNING);
                    }
                }
                break;
            }
            pcntl_signal(SIGCHLD, SIG_DFL);
        }
        return true;
    }

    /**
     * 获取作业上次的调度信息
     * @param  callable $callHandler 执行任务的调度函数
     * @return mixed
     */
    abstract public function getLastDispatcherInfo(callable $callHandler);

    /**
     * 保存本次作业的调度信息
     * @param  callable $callHandler 回调函数
     * @param  array    $arrPackets     任务包数组
     * @return mixed
     */
    abstract public function saveDispatcherInfo(callable $callHandler, array $arrPackets);

    /**
     * 加载本次执行的任务数组到调度器（产生以在redis中存储的任务id为索引，以原任务数组id为值的数组）
     * @param  array $arrTasks    组成本次作业的任务列表数组
     * @return array
     */
    public function loadArrTasks(array &$arrTasks) {
        $arrTasks = array_unique($arrTasks, SORT_REGULAR);
        $this->arrPackets = array();
        foreach ($arrTasks as $taskId => $taskObj) {
            $packetId = self::KEYWORD_TASK . '_' . $this->getPacketId($taskObj);
            $this->arrPackets[$packetId] = $taskId;
        }
        $this->arrTasks = &$arrTasks;
        return $this->arrPackets;
    }

    /**
     * 设置执行业务的时间限制(同时也是子进程的存活期)
     * @param int $duration 时长(单位：秒)
     */
    public function setProcessDuration($duration) {
        $this->jobStartTime = time();
        $this->jobEndTime   = $this->jobStartTime + $duration;
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
     * @return array
     */
    public function getPacketAllocationList(array $arrPacketIds = array()) {
        $list = $this->getPacketsFromWorkPool($arrPacketIds);
        foreach ($list as $packetId => $v) {
            $pid = intval($v['pid']);
            $isRun = self::subProcessIsRun ($pid);

            //运行期内；非本机进程或者仍在运行中
            if (time() < $v['end_time'] && ($v['node'] != $this->serverName || $isRun)) {
                if ($v['node'] == $this->serverName && !in_array($pid, $this->pids)) {
                    $this->pids[] = $pid;
                }
                continue;
            }
            $packetStatus = self::PACKET_STATUS_WRONG;
            if ($v['end_time'] < time() && $isRun) {
                $res = posix_kill($pid, SIGKILL); //结束超时进程
                if (false === $res) { //如果终止进程失败
                    trigger_error('Failed to send a kill sinal to a child process, pid:' . $pid, E_USER_WARNING);
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

            //主进程得到是创建成功的子进程pid号，返回此pid
            if ($pid > 0) {
                return $pid;
            }

            //子进程得到是0
            if ($pid == 0) {
                $this->pid = posix_getpid();
                if (false === $this->pid) {//如果得不到自己的进程pid
                    trigger_error('child process failed to use posix_getpid function to get pid.', E_USER_WARNING);
                    exit(SIGKILL);
                }
                try {
                    //将获取到的pid加入进程信息存储池；并限时子进程的时间；执行任务
                    $this->addPidToPool();
                    set_time_limit($this->jobEndTime-time());
                    MultiProcess_Child::exec($this, $handler); //已包含结束语句；子进程执行完毕后会自动退出，无需在附加退出语句
                } catch(Exception $e) {
                    $this->recordError($e->getCode(), $e->getMessage());
                    $this->closeChildProcess();
                }
                //执行到这里，说明不能通过发送信号关闭，那就调用exit
                exit(SIGKILL);
            }

            //得到pid是小于0，说明fork失败，随机暂停小于1秒的时间，在尝试fork
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
            if (is_numeric($pid) && $pid > 0 && self::subProcessIsRun($pid)) {//该进程还在运行
                if (is_numeric($endTime) && $endTime > time()) {
                    $isDel = false;
                } elseif ($endTime < time()) { //超时
                    $isDel = posix_kill($pid, SIGKILL);
                    if (false === $isDel) {
                        trigger_error('Failed to send a kill sinal to a child process, pid:' . $pid, E_USER_WARNING);
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
