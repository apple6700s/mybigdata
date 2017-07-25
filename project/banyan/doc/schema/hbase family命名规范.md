1. HBase的family和qualifier命名原则上应该尽量简短清晰
2. family比较常用，一般建议以一到两个标志性的字母作为family，统一小写
3. qualifier能缩写的采用统一的缩写，比如cnt表示count，msg表示message，auth表示权限等。（但要避免非通用性的缩写）
4. 全量库列族规定：一般数据字段用r family（表示raw），元信息和爬虫字段用m（表示meta），分析字段用a（表示analyz）。其余需要增加的列族按照英文单词的首字母缩写，有歧义的用两个字母。

5. 同义的字段应和已存在的schema的同义字段名字一致，比如 title/author/content/update_date/publish_date/publish_time/view_cnt/review_cnt/like_cnt等
