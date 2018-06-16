<?php
/**
 * @file Multiprocess_child.php
 * @author liuzhongliang(13439694341@qq.com)
 * @date 2018/05/06 10:14:51
 * @brief  多进程执行的子进程类(主要功能从调度器申请任务进行执行)
 **/
abstract class MultiProcess_Child extends MultiProcess_Base {

    /**
     * @param \MultiProcess_Scheduler_Base $objScheduler 调度器对象
     * @param callable $handler 任务包处理程序(回调函数)
     */
    final public static function exec($objScheduler, $handler){

        //从调度器申请一个任务包进行执行
        while ($packet = $objScheduler->applyTaskPacket()) {

            //调用处理函数执行任务
            $res = call_user_func($handler, $packet['task']);

            //执行结果返回值如果为false记录任务包的状态为运行完成但失败; 无返回值记录任务包的状态为成功完成
            $res = false === $res ? self::PACKET_STATUS_FAILED : self::PACKET_STATUS_COMPLETE;
            $objScheduler->savePacketStatus($packet['packetId'], $res);
        }

        //关闭子进程
        $objScheduler->closeChildProcess();
    }
}