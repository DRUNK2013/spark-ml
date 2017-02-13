#<h1 style='float:center'>spark Machine Learning 模块源码练习</h1>
#1.fpg频繁模式数据挖掘，包括关联规则、频繁模式、序列模式
####fpg包含三个模块:</br>
1.AssociationRules,输入数据集RDD[Item].把数据每项分裂成[前项,后项,频次]形式关联规则候选集.然后候选集和输入数据集进行join,并按照频次过滤,找出候选集<br>
2.FPGrowth频繁模式树Growth.其中把数据集吸写入到FPTree数据模型中,最后从FPTree抽取所有的符合频次的频繁集.<br>
#2.NLP特征提取
##TF-IDF
TF-IDF全称:term frequency - inverse doucument frequency(词频-逆向文件频率).<br>
TF-IDF是一种统计方法，用以评估一字词对于一个文件集或一个语料库中的其中一份文件的重要程度. <br>
####TF(词频)
TF在spark中,通过HashingTF实现,对每个词取Hash,并计算词频.
####IDF(逆向文件词频)
word2vec

