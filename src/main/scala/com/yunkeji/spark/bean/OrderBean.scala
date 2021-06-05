package com.yunkeji.spark.bean

case class OrderBean(
                      oid: String,
                      cid: Int,
                      money: Double,
                      longitude: Double,
                      latitude: Double,
                      var province:String,
                      var city:String
                    )
