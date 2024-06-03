# 模块总览
具体模块细节请见各模块的README.md  
其中，common、commontest、spatialtest、cvas-spark是四个模块是为了方便测试和开发而创建的，无论是继续开发小论文代码和集成到oge中都不需要特别关注  
代码以开发方便为原则，java和scala混合开发
## common
- 公用的工具、类、方法
- 之后不知道该放哪个模块的工具都可以放这里
## commontest
- 模块包含了只在测试中使用的通用类
## spatialtest
- 空间测试模块基类
## cvas-spark
- 打包相关、以及与spark集成以加快开发
## butterfly
- java api以及实验相关代码模块
## core
- 核心模块，主要是空间连接相关的模块
- 其中缓冲区和叠置相关的代码用java比较好写，放在butterfly中
## io
- 读写及划分
- 这部分当需要拓展时，可以进行研究
## visualization
- 主要用来画论文的分区、矢量切分效果展示图
- 比较简陋，并不能作为成熟的可视化工具