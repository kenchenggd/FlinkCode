package com.atguigu.flink.project.util

case class MarketingUserBehavior(userId: String,
                                 behavior: String,
                                 channel: String,
                                 ts: Long)