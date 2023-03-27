package com.example.basic

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object testWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // exactly-once 语义保证整个应用内端到端的数据一致性
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 开启检查点并指定检查点时间间隔为5s
    //env.enableCheckpointing(5000)
    env.setParallelism(1)

    //     nc -lk 9999
    //     nc -l -p 9999 windows使用需安装netCat
    val text = env.socketTextStream("localhost", 9999)

    // 打印原始日志
    text.filter(r=> !r.isEmpty).map(r=>("data:",r)).print()

    //map https://blog.csdn.net/q18729096963/article/details/107594415 （map,filter,flatMap）
    /*  //map函数

    text.map(new MapFunction[String,String] {
      override def map(value: String): String = {value + "aaaaa"};
    }).print()

    text.map(new RichMapFunction[String, String]{
      override def map(value: String): String = { value + "bbbb" }
      // 富函数提供了生命周期方法
      override def open(parameters: Configuration): Unit = {}
      override def close(): Unit = {}
    }).print()

    val flatMap_a = text.flatMap(
     new FlatMapFunction[String, String]() {
      def flatMap(urlInfos: String, collector: Collector[String]): Unit = {
        for (word <- urlInfos.split(",")) {
          collector.collect(word)
        }
      }
    }).print()
    */

    //filter
    /*
    val fileter_a = text.filter(
      new FilterFunction[String] {
        override def filter(line: String): Boolean = {
          line.split(",")(2).toDouble > 3
        }
      }
    ).map(a=> a+"filter").print()
    */

    //keyBy 构造类 min不关心无关列,minby保留无关列
    /*
    val class_a = text.map(lines =>  {
          val s_a = lines.split(",")
          (s_a(0), s_a(1).toLong, s_a(2).toInt)
          //WaterSensor(s_a(0), s_a(1).toLong, s_a(2).toInt)
        })
    val key_a  = class_a.keyBy(0,1).sum(2).map(a=>a.toString()+'sum).print()
    val key_b  = class_a.keyBy(0,1).min(2).map(a=>a.toString()+'min).print()
    val key_c  = class_a.keyBy(0,1).minBy(2).map(a=>a.toString()+'minby).print()
    */

    //分发 global、broadcast、forward、shuffle、rebalance、rescale、partitionCustom
    /*
    val class_a = text.map(lines =>  {
          val s_a = lines.split(",")
          (s_a(0), s_a(1).toLong, s_a(2).toInt)
          //WaterSensor(s_a(0), s_a(1).toLong, s_a(2).toInt)
        })
    val key_a  = class_a
      .global //全部发往第1个task
      .broadcast //广播
      .forward //上下游并发度一样时一对一发送
      .shuffle //随机均匀分配
      .rebalance //Round-Robin(轮流分配)
      .rescale //Local Round-Robin(本地轮流分配)
      .partitionCustom(new Partitioner[Long]{
          override def partition(key: Long, numpartition: Int): Int = {
            System.out.println("总的分区数"+numpartition)
            if (key%2==0){
              0
            }else{
              1
            }
          }
        },0
      ) //自定义单播
    //分区之后的数据
    val result = key_a.map(line => {
      println("当前线程id" + Thread.currentThread().getId + ",value" + line)
      line._1
    })
    result.print().setParallelism(1)
    */

    //reduce https://blog.csdn.net/dinghua_xuexi/article/details/107766222
    /*
    val c = text
            .map(r=>r)
            //.flatMap(r=>r.split(","))
            .map((_,1))
            .keyBy(0)
            //.reduce((x, y) => { (x._1, x._2 + y._2) })
           .reduce(new RichReduceFunction[(String,Int)]{
             override def open(parameters: Configuration): Unit = super.open(parameters)
             override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
               (value1._1, value1._2 + value2._2)
             }
            })
            .print()
    */

    //fold
    /*
    val class_a = text.filter(r=> !r.isEmpty).map(lines =>  {
          val s_a = lines.split(",")
          (s_a(0), s_a(1).toLong, s_a(2).toInt)
          //WaterSensor(s_a(0), s_a(1).toLong, s_a(2).toInt)
        })
    val key_a  = class_a.keyBy(0).fold(0)((str, i) => { str + i._3 }).print()
    */

    //split 分流, select , union 合流(多个相同流,FIFO,不去重) https://zhuanlan.zhihu.com/p/99425612
    /*
    val split_data = text.split(
      (num: String) => (num == "a") match {
        case true => List("even")
        case false => List("odd")
      }
    )
    //获取分流后的数据
    val select_even: scala.DataStream[String] = split_data.select("even")
    val select_odd: scala.DataStream[String] = split_data.select("odd")
    select_even.map(r=>r+"even").print()
    select_odd.map(r=>r+"odd").print()
    select_even.union(select_odd).map(r=>r+"union").print()
    */

    //connect https://zhuanlan.zhihu.com/p/99425612 (两个不同流)
    /*
    val intStream = env.fromElements(1, 0, 9, 2, 3, 6)
    val stringStream = env.fromElements("LOW", "HIGH", "LOW", "LOW")
    val connectedStream  = intStream.connect(stringStream)
    // CoMapFunction三个泛型分别对应第一个流的输入、第二个流的输入，map之后的输出
    class MyCoMapFunction extends CoMapFunction[Int, String, String] {
      override def map1(input1: Int): String = input1.toString
      override def map2(input2: String): String = input2
    }
    val mapResult = connectedStream.map(new MyCoMapFunction)
    mapResult.print()
    */

    //process 环境相关信息,时间戳信息和水位线信息 https://www.jianshu.com/p/ca67629d8297
    /*
    val class_a = text.map(lines =>  {
          val s_a = lines.split(",")
          //(s_a(0), s_a(1).toLong, s_a(2).toInt)
          WaterSensor(s_a(0), s_a(1).toLong, s_a(2).toInt)
        })
    val key_a  = class_a.keyBy(_.id).process(new MyKeyedProcessFunction).print()
    */

    //iterate 需注释Checkpoint https://blog.csdn.net/qq864181762/article/details/106581595
    /*
    val iterate = text.iterate(
        stepFunction =>{
          val feedback: scala.DataStream[String] = stepFunction.filter(s=>s=="haha").setParallelism(1)
          feedback.print()
          val output: scala.DataStream[String] = stepFunction.filter(s=>s!="haha")
          (feedback,output)
        }
      )
      .print()
    */

    //Window  http://wuchong.me/blog/2016/05/25/flink-internals-window-mechanism/
    /*
       //.trigger()  - 触发器
       //.evictor()  - 移除器
       //.allowedLateness() – 允许处理迟到的数据
       //.sideOutputLateData(lateOutputTag) – 将迟到的数据流放入侧输出流
       //.getSideOutput(lateOutputTag) – 获取侧输出流

      val c = text.map(r=>r)
          .flatMap(r=>r.split(","))
          .map((_,1))
      // 打印清洗后的数据
      //c.map(r=>("clean:",r)).print()

      c.keyBy(0)
        .timeWindow(Time.seconds(10), Time.seconds(3)) //Sliding滑动,每个3秒统计过去10秒
        //.timeWindow(Time.seconds(10)) //Tumblin滚动
        //.countWindow(5) 窗口数量 默认使用的是 processing time
        //.countWindow(5, 2) //每2个元素统计过去5个元素的数量之和：
        //基于Event Time，每5秒内的数据为界，以每秒的滑动窗口速度进行operator操作，但是，当且仅当5秒内的元素数达到8时，才触发窗口，触发时保留上个窗口的2个元素。
        //.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1))).trigger(CountTrigger.of(8)).evictor(CountEvictor.of(10))
        //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(4))) //用户4秒没有活动则视为会话断开
        .sum(1)
        .map(r=>("result:",r))
        .print()
    */

    env.execute("Custom Source")
  }

  case class WaterSensor(id: String, ts: Long, vc: Double)

  // 自定义KeyedProcessFunction,是一个特殊的富函数
  // 1.实现KeyedProcessFunction，指定泛型：K - key的类型， I - 上游数据的类型， O - 输出的数据类型
  // 2.重写 processElement方法，定义 每条数据来的时候 的 处理逻辑
  class MyKeyedProcessFunction extends KeyedProcessFunction[String, WaterSensor, String] {
    /**
     * 处理逻辑：来一条处理一条
     *
     * @param value 一条数据
     * @param ctx   上下文对象
     * @param out   采集器：收集数据，并输出
     */
    override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
      out.collect("我来到process啦，分组的key是="+ctx.timestamp()+",数据=" + value)
      // 如果key是tuple，即keyby的时候，使用的是 位置索引 或 字段名称，那么key获取到是一个tuple
      //      ctx.getCurrentKey.asInstanceOf[Tuple1].f0 //Tuple1需要手动引入Java的Tuple
    }
  }

}

//  https://www.coder.work/article/6500489