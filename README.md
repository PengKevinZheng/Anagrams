# Anagrams

  一本英文书籍包含成千上万个单词或者短语，现在我们需要在大量的单词中，找出相同字母组成的所有单词。  
  
  1. map阶段： 
  
      1.map的输入key： 每行偏移量  IntWritable
  
      2.map的输入value：每个单词  Text
  
      3.map的输出key：字母经过去重，排序之后的字符串  Text
  
      4.map的输出value：该单词 Text
  
  去重：讲字符串中的每个字符存入hashset中
  
  排序： 对Array中的每个元素排序： Arrays.sort(arr)
  
  
  2. reduce阶段  
      
      1.reduce的输入key： 同map的输出key   Text
      
      2.reduce的输入value： 同map的输出value   Iterable<text>
      
      3.reduce的输出key：字母经过去重，排序之后的字符串  Text
      
      4.reduce的输出value： 所有组成字母相同的单词的集合  Text
  
  
