<?php
/**
 * @file   RedisController.php
 * @author 刘重量(13439694341@qq.com)
 * @date   2018/05/06 10:14:51
 * @brief  多进程执行类的redis控制类
 **/
class MultiProcess_Scheduler_By_Redis extends MultiProcess_Scheduler_Base {

    const REDIS_SERVER     = '127.0.0.1';
    const REDIS_PORT       = 6379;
    const KEY_TASK_TABLE   = 'MultiProcess_Task_Pool';      //存储以序列化后任务的值为键名，唯一id号为值的哈希表数组
    const KEY_ID_GENERATOR = 'MultiProcess_Id_GENERATOR';   //用做id发生器的redis的key
	const DEF_TTL          = 3600;                          //默认任务表key的生存时间
    const MAX_WAIT_TIME    = 1000000;                       //每次重试最大等待时间

    private $hKey_ProcessPool_table = '';    //进程信息表名; redis的key;目前只存储以子进程pid为键名, 过期时间戳为值的哈希表数组
    private $hKey_JobInfo_table     = '';    //作业信息表; redis的key, 保存上次作业调度信息的表
    private $hKey_WorkPool_table    = '';    //工作池表名; redis哈希表key; 存储以任务名为key,处理子进程pid为value的数组
    private $hKey_TaskFinish_table  = '';    //成员任务完成时间表名;   redis哈希表key; 存储以任务名为索引,完成时间戳为值的数组
    private $hKey_TaskResult_table  = '';    //成员任务执行结果表名;   redis哈希表key;  存储以任务名索引，执行结果为值的数组
    private $hKey_RunTime_table     = '';    //成员任务耗时表名 （哈希表Key, 存储子任务pid为key,花费时间为value的数组)

    /**
     * @var \Redis  redis原生对象
     */
    private static $objRedis = null;

    /**
     * 初始化函数
     */
    public function init() {
        $serverName = $this->serverName;
        $appName    = self::APP_NAME . '[' . $serverName . ']'; //项目名加机器名前缀;确保唯一性;可读性

        //保存项目子进程信息的表key
        $this->hKey_ProcessPool_table = $appName . '[' . self::KEYWORD_PROCESS . ']';
        if (!empty($this->workName)) {
            //任务类型名标识（执行程序相同说明是同一类型的任务）
            $this->workName = self::APP_NAME . '[' . $this->workName . ']';
            $this->hKey_JobInfo_table = self::APP_NAME . '[job_info]';

            $this->hKey_WorkPool_table   = $this->workName . '[' . self::KEYWORD_TASK . '_pool'    . ']';   //redis key; 存储正在被子进程执行的任务包信息表
            self::$objRedis->expire($this->hKey_WorkPool_table, self::DEF_TTL);
            $this->hKey_TaskFinish_table = $this->workName . '[' . self::KEYWORD_TASK . '_endtime' . ']';   //redis key; 存储任务包完成时间的信息表
			self::$objRedis->expire($this->hKey_TaskFinish_table, self::DEF_TTL);
            $this->hKey_RunTime_table    = $this->workName . '[' . self::KEYWORD_TASK . '_result'  . ']';   //redis key; 存储处理成员任务的耗时信息的表
			self::$objRedis->expire($this->hKey_RunTime_table, self::DEF_TTL);
            $this->hKey_TaskResult_table = $this->workName . '[' . self::KEYWORD_TASK . '_cost'    . ']';   //redis key; 存储被处理过的任务包状态的表
			self::$objRedis->expire($this->hKey_TaskResult_table, self::DEF_TTL);
            $this->jobStartTime = time();         //设置作业开始执行的时间戳;     默认值：当前时间
            $this->jobEndTime   = time() + 86400; //设置作业可被结束运行的时间戳; 默认值：24小时后
        }
        self::renewRedisConnect(); //刷新redis连接
    }

    /**
     * 获取作业上次的调度信息
     * @param  callable $callHandler 回调函数
     * @return mixed
     */
    public function getLastDispatcherInfo(callable $callHandler) {
        $scope = static::getScope();
        $jobId = array(
            'namespace' => $scope,
            'callback'  => $callHandler,
        );
        $jobId = serialize($jobId);
        $jobInfo = $this->hGet($this->hKey_JobInfo_table, $jobId);
        if(!empty($jobInfo)) {
            $jobInfo = json_decode($jobInfo, true);
        }
        if (empty($jobInfo)) {
            return array();
        }
        $this->jobStartTime = $jobInfo['startTime'];
        $this->jobEndTime   = $jobInfo['endTime'];
        $this->arrPackets   = $jobInfo['arrPackets'];
        return $jobInfo['arrPackets'];
    }

    /**
     * 保存本次作业的调度信息
     * @param  callable $callHandler 回调函数
     * @param  array $arrPackets
     * @return mixed
     */
    public function saveDispatcherInfo(callable $callHandler, array $arrPackets) {
        $scope = static::getScope();
        $jobId = array(
            'namespace' => $scope,
            'callback'  => $callHandler,
        );
        $jobId = serialize($jobId);
        $jobInfo = array(
            'arrPackets' => $arrPackets,
            'startTime'  => $this->jobStartTime,
            'endTime'    => $this->jobEndTime,
        );
        $jobInfo = json_encode($jobInfo);
        $this->hSet($this->hKey_JobInfo_table, $jobId, $jobInfo);
    }

    /**
     * 返回本类程序所开的所有子进程数
     * @return int
     */
    public function getAllProcessNum() {
        $num = self::hLen($this->hKey_ProcessPool_table);
        if ($num !== false && $num < self::FORK_LIMIT) {
            return $num;
        }
        $this->cleanDefunctProcess();  //清理已经结束的子进程和超时运行的子进程
        $num = self::hLen($this->hKey_ProcessPool_table);
        return $num;
    }

    /**
     * 返回处理后等待分配子进程执行的任务数组（过滤掉已执行完成的任务和已分配子进程的任务）
     * @return array  未分配执行进程的任务数组
     */
    public function getWaitPackets() {
        if (empty($this->arrPackets)) {
            return array();
        }
        $processingTaskList = $this->getPacketAllocationList();
        if (!empty($processingTaskList)) {  //过滤掉数组中已分配处理子进程的任务
            $this->arrPackets = array_diff_key($this->arrPackets, $processingTaskList);
        }
        if (!empty($this->arrPackets)) {
            $finishPackets = $this->getFinishPackets();
            if (!empty($finishPackets)) { //过滤掉数组中已完成的任务名称元素
                $this->arrPackets = array_diff_key($this->arrPackets, $finishPackets);
            }
        }
        return $this->arrPackets;
    }

    /**
     * 申请一个任务唯一分配给$pid对应的子进程执行(每进程每次只能申请到一个任务)
     * @return array
     */
    public function applyTaskPacket() {
        $this->getWaitPackets();
        //如果没有未分配的任务（所有任务已分配结束）,或者子进程限期已过，返回空值
        if (count($this->arrPackets) == 0 || $this->jobEndTime < time()) {
            return null;
        }
        foreach ($this->arrPackets as $packetId => $taskId) {
            $applyTime = time(); //申请时间

            //打包封装子进程信息
            $processInfo = array(
                'node'       => $this->serverName,   //服务器名
                'pid'        => $this->pid,          //子进程pid
                'apply_time' => $applyTime,          //申请任务包的时间
                'end_time'   => $this->jobEndTime,
            );
            $processInfo = json_encode($processInfo);

            //分配任务包给子进程
            $res = $this->hSetNx($this->hKey_WorkPool_table, $packetId, $processInfo);
            if ($res) { //设置成功
                $finishTasks = $this->getFinishPackets(); //获取本次作业中已完成的任务名成员列表
                if (isset($finishTasks[$packetId])) {
                    $this->delPacketFromWorkPool($packetId);
                    unset($this->arrPackets[$packetId]);
                } else {//不存在于已完成列表中
                    $this->arrPacketSendTime[$packetId] = $applyTime; //记录发包时间
                    $taskId = $this->arrPackets[$packetId];
                    unset($this->arrPackets[$packetId]);
                    return array('packetId' => $packetId, 'task' => $this->arrTasks[$taskId]); //返回申请到的任务包
                }
            } elseif ($this->hExists($this->hKey_WorkPool_table, $packetId)) {
                unset($this->arrPackets[$packetId]);
            }
        }
        return null; //如果所有任务都已分配完或者在允许重试次数限制内申请不到任务，返回空值
    }

    /**
     * 返回所在服务器（网络）节点内子进程信息表数组(key:进程pid;  vaule: 过期时间戳)
     * @return array
     */
    protected function getAllProcessInfo() {
        $list = $this->hGetAll($this->hKey_ProcessPool_table);
        return $list;
    }

    /**
     * 保存子进程的pid到进程池
     * @return  bool
     */
    protected function addPidToPool() {
        $res = $this->hSet($this->hKey_ProcessPool_table, $this->pid, $this->jobEndTime);
        return $res;
    }

    /**
     * 从进程信息表中删除一个子进程的pid 记录
     * @param int $pid
     */
    protected function delPidFromPool($pid) {
        $this->hDel($this->hKey_ProcessPool_table, $pid);
    }

    /**
     * 返回来自已保存的任务分配表中记录信息
     * @param  array $arrPacketNames 指定的任务包查询范围
     * @return array
     */
    public function getPacketsFromWorkPool(array $arrPacketNames = array()) {
        $arrPacketNames = empty($arrPacketNames) ? array_keys($this->arrPackets) : $arrPacketNames;
        if (empty($arrPacketNames)) {
            return array();
        }
        $list = $this->hMget($this->hKey_WorkPool_table, $arrPacketNames);
        foreach($list as $packetName => $processInfo) {
            $v = json_decode($processInfo, true);
            if (isset($v['node']) && isset($v['pid']) && is_numeric($v['pid']) && isset($v['apply_time']) && is_numeric($v['apply_time']) && isset($v['end_time']) && is_numeric($v['end_time']) && $v['end_time'] > $v['apply_time']) {
                $list[$packetName] = $v;
            } else {
                $this->hDel($this->hKey_WorkPool_table, $packetName);
                unset($list[$packetName]);
            }
        }
        return $list;
    }

    /**
     * 从工作池表中删除一个任务包
     * @param string $packetName 被删除的任务包名（对应的表键）
     * @return bool
     */
    public function delPacketFromWorkPool($packetName) {
        $this->hDel($this->hKey_WorkPool_table, $packetName);
    }

    /**
     * 保存指定任务的执行信息（完成时间、耗时)
     * @param string $taskName  任务名
     * @param mixed  $res       任务执行结果
     * @param int    $startTime 任务运行的开始时间
     */
    public function savePacketStatus($taskName, $res, $startTime = 0) {
        $startTime = empty($startTime) ?  $this->arrPacketSendTime[$taskName] : $startTime;
        $endTime   = time();                                                      //任务运行的结束时间
        $this->hSet($this->hKey_TaskFinish_table, $taskName, $endTime);       //保存指定任务的本次执行完成的结束时间
        $this->hSet($this->hKey_TaskResult_table, $taskName, $res);           //保存任务执行结果
        $this->hSet($this->hKey_RunTime_table, $taskName, $endTime - $startTime); //保存指定任务的执行耗时信息到任务耗时表
        $this->delPacketFromWorkPool($taskName);
    }

    /**
     * 获取已执行结束的任务的名称(key)及结束执行时间(value)组成的数组列表
     * @param  array $arrPacketNames 指定的任务包查询范围
     * @return array
     */
    public function getFinishPackets(array $arrPacketNames = array()) {
        $arrPacketNames = empty($arrPacketNames) ? array_keys($this->arrPackets) : $arrPacketNames;
        if (empty($arrPacketNames)) {
            return array();
        }
        $list = $this->hMget($this->hKey_TaskFinish_table, $arrPacketNames);
        $finishTasks = array();
        foreach ($list as $packetName => $endTime) {
            if (isset($endTime) && is_numeric($endTime) && $endTime > $this->jobStartTime) {
                $finishTasks[$packetName] = $endTime;
            }
        }
        return $finishTasks;
    }

    /**
     * 获取已完成模块的执行结果数组（key:模块名; value: 执行结果)
     * @param  array $arrPacketNames 指定的任务包查询范围
     * @return array
     */
    public function getFinishPacketResults(array $arrPacketNames = array()) {
        $arrPacketNames = empty($arrPacketNames) ? array_keys($this->arrPackets) : $arrPacketNames;
        if (empty($arrPacketNames)) {
            return array();
        }
        $finishTasks = $this->getFinishPackets($arrPacketNames);
        $arrPacketName = array_keys($finishTasks);
        $list = $this->hMget($this->hKey_TaskResult_table, $arrPacketName);
        foreach($list as $packetId => $result) {
            if(empty($result)) {
                $list[$packetId] = self::PACKET_STATUS_WRONG;
            }
        }
        return $list;
    }

    /**
     * 返回由任务名为key,对应上次执行花费时间的值为value的数组
     * @return array
     */
    public function getPacketHandlerTimeList() {
        $list = $this->hMget($this->hKey_RunTime_table,array_keys($this->arrPackets));
        foreach($list as $k => $v) {
            if(!is_numeric($v)) {
                unset($list[$k]);
            }
        }
        return $list;
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
     * 返回任务在redis里的存储id
     * @param string $task
     * @return bool|int
     */
    protected function getPacketId($task) {
        $packet = serialize($task);
        $id     = $this->hGet(self::KEY_TASK_TABLE, $packet);
        if (is_numeric($id) && $id > 0) {
            return intval($id);
        }
        $id = self::$objRedis->incr(self::KEY_ID_GENERATOR);
        if (empty($id) || !is_numeric($id)) {
            self::$objRedis->set(self::KEY_ID_GENERATOR, 0);
            $id = self::$objRedis->incr(self::KEY_ID_GENERATOR);
            if (empty($id) || !is_numeric($id)) {
                trigger_error('Redis can not be used normally!', E_USER_WARNING);
                throw new Exception('redis can not use', self::ERROR_REDIS_WRONG);
            }
        }
        $res = $this->hSetNx(self::KEY_TASK_TABLE, $packet, $id);
        if ($res) {
            return $id;
        }
        //设置不成功也可能是同时有其它程序为该任务设置了唯一id,所以要取出值判断一下
        $id = $this->hGet(self::KEY_TASK_TABLE, $packet);
        if (!empty($id) && is_numeric($id) && $id>0) {
            return intval($id);
        }
        throw new Exception('获取任务id失败', self::ERROR_REDIS_WRONG);
    }
    /**
     * 以下为封装的redis方法
     */

    /**
     * 返回表行数
     * @param string $table 表名
     * @return int
     */
    private static function hLen($table) {
        $len = (self::$objRedis instanceof Redis) ? self::$objRedis->hLen($table) : false;
        if (false === $len) {
            self::renewRedisConnect();
            $len = self::$objRedis->hLen($table);
            if (false === $len) {
                trigger_error('Redis can not be used normally!', E_USER_WARNING);
                throw new Exception('redis can not use', self::ERROR_REDIS_WRONG);
            }
        }
        return $len;
    }

    /**
     * 获取表中指定的字段是否存在
     * @param string $table
     * @param string $field
     * @return bool
     */
    private function hExists($table, $field) {
        self::renewRedisConnect();
        return self::$objRedis->hExists($table, $field);
    }

    /**
     * 返回对应存储在redis哈希表
     * @param string $table      存储在redis哈希表的key
     * @param int    $retryCount 重试次数
     * @return array
     */
    private function hGetAll($table, $retryCount = self::RETRY_COUNT) {
        $list = (self::$objRedis instanceof Redis) ? self::$objRedis->hGetAll($table) : false;
        if (false === $list) {
            self::renewRedisConnect();
            $list = self::$objRedis->hGetAll($table);
            if (false === $list) {
                trigger_error('Redis can not be used normally!', E_USER_WARNING);
                throw new Exception('redis con not use', self::ERROR_REDIS_WRONG);
            }
        }
        return $list;
    }

    /**
     * 返回指定数组key存储在redis中对应的value
     * @param string $table      表名，redis键
     * @param array  $fields     字段
     * @return array
     */
    private function hMget($table, array $fields) {
        if (empty($fields)) {
            return array();
        }
        $list = (self::$objRedis instanceof Redis) ? self::$objRedis->hMGet($table, $fields) : false;
        if (false === $list) {
            self::renewRedisConnect();
            $list = self::$objRedis->hMGet($table, $fields);
            if (false === $list) {
                trigger_error('Redis can not be used normally!', E_USER_WARNING);
                throw new Exception('redis can not use', self::ERROR_REDIS_WRONG);
            }
        }
        return $list;
    }

    /**
     * 获取指定的表中字段的存储值
     * @param string $table
     * @param string $field
     * @return string
     */
    private function hGet($table,$field) {
        $table = trim($table);
        $field = trim($field);
       
        $retryCount = self::RETRY_COUNT;
        do {
            try{ 
				if (!(self::$objRedis instanceof Redis)) {
					self::renewRedisConnect();
				}
                if (!(self::$objRedis->hExists($table,$field))) {
                    return null;
                }
                return self::$objRedis->hGet($table, $field);
            }
            catch (Exception $e) {
                --$retryCount;
                if ($retryCount == 0) {
                    throw new Exception($e->getMessage(),$e->getCode());
                }
                $seconds = rand(floor(self::MAX_WAIT_TIME * $retry/self::MAX_WAIT_TIME),self::MAX_WAIT_TIME);
                usleep($seconds);
                self::renewRedisConnect();
            }
        }while ($retryCount);
    }

    /**
     * 设置指定表中指定字段的对应值
     * @param string $table 表名
     * @param string $field 表字段名
     * @param string $value 对应值
     * @return bool
     */
    private function hSet($table, $field, $value) {
        if (!is_string($table) || !is_string($field) || !is_string($value)) {
            trigger_error('pass wrong params', E_USER_ERROR);
        }
        $table = trim($table);
        $field = trim($field);
        if ($table == '' || $field == '') {
            trigger_error('pass wrong params', E_USER_ERROR);
        }
        $isRetry = false;
        do {
            try{
                if (!(self::$objRedis instanceof Redis)) {
                    self::renewRedisConnect();
                }
                $res = self::$objRedis->hSet($table, $field, $value);
            } catch (Exception $e) {
                if (false === $isRetry) {
                    usleep(rand(0, self::MAX_WAIT_TIME));
                    self::renewRedisConnect();
                }
                $res = false;
            }
            if ($res !== false || $isRetry) {
                return $res !== false;
            }
        } while(false === $isRetry && $isRetry = true);
    }

    /**
     * 指定表中插入新值
     * @param string $table 表名
     * @param string $field 新字段名
     * @param string $value 对应值
     * @return bool
     */
    private function hSetNx($table, $field, $value) {
        if (!is_string($table) || !is_string($field) || !is_string($value)) {
            trigger_error('pass wrong params', E_USER_ERROR);
        }
        $table = trim($table);
        $field = trim($field);
        if ($table == '' || $field == '') {
            trigger_error('pass wrong params', E_USER_ERROR);
        }
        $isRetry = false;
        do {
            try{
                if (!(self::$objRedis instanceof Redis)) {
                    self::renewRedisConnect();
                }
                $res = self::$objRedis->hSetNx($table, $field, $value);
            } catch (Exception $e) {
                if (false === $isRetry) {
                    usleep(rand(0, self::MAX_WAIT_TIME));
                    self::renewRedisConnect();
                }
                $res = false;
            }
            if ($res !== false || $isRetry) {
                return $res;
            }
        } while(false === $isRetry && $isRetry = true);
    }

    /**
     * 删除指定表中的指定项
     * @param string $table 表名
     * @param string $field 项名
     * @return bool
     */
    private function hDel($table, $field) {
        $retry = self::RETRY_COUNT;
        $msg = 'Redis delete an hash field Failed!';
        do {
            try{
                $res = (self::$objRedis instanceof Redis) ? self::$objRedis->hDel($table, $field) : false;
            } catch (Exception $e) {
                if (--$retry >= 0) {
                    $waitTime = rand(floor(self::MAX_WAIT_TIME * $retry/self::MAX_WAIT_TIME),self::MAX_WAIT_TIME);
                    usleep($waitTime);
                }
                $msg = $e->getMessage() . ', code:' . $e->getCode();
            }
            if (false === $res) {
                self::renewRedisConnect();
            } else {
                return true;
            }
        } while ($retry > 0);
        trigger_error($msg, E_USER_ERROR);
        //throw new Exception($msg, self::ERROR_REDIS_WRONG);
    }
    
    /**
     * 刷新redis连接
     */
    private static function renewRedisConnect() {
        $retryCount = self::RETRY_COUNT;
        $msg = 'Redis 无法正常使用!';
        $code = MultiProcess_Base::ERROR_REDIS_WRONG;
        do {
            try {
                $isConnect = true;
                if (!(self::$objRedis instanceof Redis)) {
                    self::$objRedis = new Redis();
                    $isConnect = self::$objRedis->connect(self::REDIS_SERVER, self::REDIS_PORT);
                }
                if ($isConnect && '+PONG'==self::$objRedis->ping()) {
                    return true;
                }
            } catch (Exception $e) {
                $msg = $e->getMessage();
                $code =$e->getCode();
            }
            if (--$retryCount >= 0) {
                $waitTime = rand(floor(self::MAX_WAIT_TIME * $retryCount/self::MAX_WAIT_TIME),self::MAX_WAIT_TIME);
                usleep($waitTime);
            }
        }while($retryCount > 0);
        trigger_error('Redis can not be used normally!', E_USER_WARNING);
        throw new Exception($msg, $code);
    }
}