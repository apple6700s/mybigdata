## MIG接口

### SDK接口：

#### RhinoNewsForumSearcher

1. RhinoNewsForumSearcher

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

2. RhinoNewsForumSearcher

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* retrieveOriginalContent: boolean, 是否获取原始内容
	* 返回值：
		* 无

3. count

	* 功能：在dt-rhino-newsforum-index索引库根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* indexType: String, 索引类型
	* 返回值：
		* long，满足条件的数量

4. countMainPost

	* 功能：在dt-rhino-newsforum-index索引库post索引类型中根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* long，满足条件的数量

5. countReply

	* 功能：在dt-rhino-newsforum-index索引库comment索引类型中根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* long，满足条件的数量

6. search

	* 功能：在dt-rhino-newsforum-index索引中根据条件查询索引
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
		* indexType： String, 索引类型
	* 返回值：
		* SearchHit[], 命中的索引结果

7. searchMainPost

	* 功能：在dt-rhino-newsforum-index索引库post索引类型中根据条件查询
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
	* 返回值：
		* List<TargetNewsForum>, 命中的新闻长文本

8. searchReply

	* 功能：在dt-rhino-newsforum-index索引库comment索引类型中根据条件查询
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
	* 返回值：
		* List<TargetNewsForumComment>，命中的新闻回复

9. aggregation

	* 功能：在dt-rhino-newsforum-index索引库根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
		* indexType: String，索引类型
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

10. aggregationOnMainPost

	* 功能：在dt-rhino-newsforum-index索引库post索引类型中根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

11. aggregationOnReply

	* 功能：在dt-rhino-newsforum-index索引库comment索引类型中根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

#### RhinoWeiboCommentSearcher

1. RhinoWeiboCommentSearcher

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

2. RhinoWeiboCommentSearcher

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* retrieveOriginalContent: boolean, 是否获取原始内容
	* 返回值：
		* 无

3. count

	* 功能：在dt-rhino-weibo-comment-index索引库根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* indexType: String, 索引类型
	* 返回值：
		* long，满足条件的数量

4. countWeibo

	* 功能：在dt-rhino-weibo-comment-index索引库weibo索引类型中根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* long，满足条件的数量

5. countComment

	* 功能：在dt-rhino-weibo-comment-index索引库comment索引类型中根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* long，满足条件的数量

6. search

	* 功能：在dt-rhino-weibo-comment-index索引库根据条件查询索引
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
		* indexType： String, 索引类型
	* 返回值：
		* SearchHit[], 命中的索引结果

7. searchWeibo

	* 功能：在dt-rhino-weibo-comment-index索引库weibo索引类型中根据条件查询
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
	* 返回值：
		* List<TargetNewsForum>, 命中的新闻长文本

8. searchComment

	* 功能：在dt-rhino-weibo-comment-index索引库comment索引类型中根据条件查询
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
	* 返回值：
		* List<TargetNewsForumComment>，命中的新闻回复

9. aggregation

	* 功能：在dt-rhino-weibo-comment-index索引库根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
		* indexType: String，索引类型
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

10. aggregationOnWeibo

	* 功能：在dt-rhino-weibo-comment-index索引库weibo索引类型中根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

11. aggregationOnWeiboComment

	* 功能：在dt-rhino-weibo-comment-index索引库comment索引类型中根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

#### RhinoWeiboSearcher

1. RhinoWeiboSearcher

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

2. RhinoWeiboSearcher

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* retrieveOriginalContent: boolean, 是否获取原始内容
	* 返回值：
		* 无

3. count

	* 功能：在dt-rhino-weibo-index索引库根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* indexType: String, 索引类型
	* 返回值：
		* long，满足条件的数量

4. countWeibo

	* 功能：在dt-rhino-weibo-index索引库weibo索引类型中根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* long，满足条件的数量

5. countUser

	* 功能：在dt-rhino-weibo-index索引库user索引类型中根据条件查询数量
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* long，满足条件的数量

6. search

	* 功能：在dt-rhino-weibo-index索引库根据条件查询索引
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
		* indexType： String, 索引类型
	* 返回值：
		* SearchHit[], 命中的索引结果

7. searchWeibo

	* 功能：在dt-rhino-weibo-index索引库weibo索引类型中根据条件查询
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
	* 返回值：
		* List<TargetNewsForum>, 命中的新闻长文本

8. searchUser

	* 功能：在dt-rhino-weibo-index索引库user索引类型中根据条件查询
	* 是否静态：否
	* 接收参数：
		* queryBuilder: QueryBuilder, 查询条件
		* offset: int, 偏移量
		* size： int, 获取数量
		* sortField： String, 排序字段
		* order： SortOrder，排序方法
	* 返回值：
		* List<TargetNewsForumComment>，命中的新闻回复

9. aggregation

	* 功能：在dt-rhino-weibo-index索引库根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
		* indexType: String，索引类型
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

10. aggregationOnWeibo

	* 功能：在dt-rhino-weibo-index索引库weibo索引类型中根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

11. aggregationOnUser

	* 功能：在dt-rhino-weibo-index索引库user索引类型中根据条件查询聚合结果
	* 是否静态：否
	* 接收参数：
		* aggregationBuilders: List<AbstractAggregationBuilder>, 聚合条件
		* queryBuilder: QueryBuilder, 查询条件
	* 返回值：
		* Map<String, Aggregation>, map<聚合名称，聚合结果>

12. getWeiboDoc

	* 功能：在dt-rhino-weibo-index索引库weibo索引类型中根据父子文档ID查询微博
	* 是否静态：否
	* 接收参数：
		* parentId: String, es父文档ID
		* childId: String, es子文档ID
	* 返回值：
		* Map<String, Object>, 根据父子文档ID查询得到的微博

13. getUserDoc

	* 功能：在dt-rhino-weibo-index索引库user索引类型中根据uid查询微博用户信息
	* 是否静态：否
	* 接收参数：
		* uid: String, 微博用户ID
	* 返回值：
		* Map<String, Object>, 微博用户信息