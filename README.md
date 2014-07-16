# 新词发现算法的基本实现以及分布式实现

> 基于Java实现。

 
该项目是基于Hadoop框架的一个Java文件集合，用于构建一个分布式的新词发现和简单的分析框架。
实现原理是通过检查一个一元词组，并与二元词组进行对比，考虑到构成背景的溯源。

本项目中的mapred部分主要有三个作业：
* 第一个作业用于计算每个词语的前景核背景数量；
* 第二个作业将一元词组和二元词组的次数联结起来；
* 第三个作业计算"phraseness"和"informativeness"的统计数据，用于识别最有意义的词语。


```java
To run the job:

    hadoop jar PhraseFinding.jar -D unigram=/path/to/unigram/file -D bigram=/path/to/bigram/file -D output=/output/dir [-D reducers=10]

其中，unigram是一元语法或者单个词语，bigram是二元语法或者双词
```
    