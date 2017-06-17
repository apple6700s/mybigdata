## Durian接口

### Http接口：

1. 微博搜索接口

	* 请求：		GET		/durian/api/status/search
	* 接收参数：
		* q：检索关键字。支持多关键字与关系检索，用空格隔开
		* startime：时间起始时间戳, 秒
		* endtime：时间截止时间戳, 秒
		* count:检索数据条数，最大只允许10000条
		* appkey:识别调用方
	* 请求示例：
		* http://internal.api.durian.datatub.com/durian/api/status/search?q=小米&starttime=1468341900&endtime=1468834512&count=10&appkey=yili
	* 返回值：Map<String, Object>
		
			{
				"success": boolean，true为正常返回，false为异常
				"total_number": int，满足条件的记录数
				"statuses": list<Status>，微博内容列表，每条微博中的字段见附录
				"msg": string，当success=false时返回的异常信息(注：异常信息未做详细处理)
			}


2. 获取符合条件的微博数量

	* 请求：		GET		/durian/api/status/count
	* 接收参数：
		* q：检索关键字。支持多关键字与关系检索，用空格隔开
		* startime：时间起始时间戳, 秒
		* endtime：时间截止时间戳, 秒
		* appkey:识别调用方
	* 请求示例：
		* http://internal.api.durian.datatub.com/durian/api/status/count?q=小米&starttime=1468341900&endtime=1468834512&appkey=yili
	* 返回值：Map<String, Object>

			{
				"success": boolean, true为正常返回，false为异常
				"total_number": int, 满足条件的记录数
				"msg": string, 当success=false时返回的异常信息(注：异常信息未做详细处理)
			}


3. 获取符合条件的微博数量（复杂参数）

	* 请求：		GET		/durian/api/v2/statuses/search/count
	* 接收参数：
		* q：检索关键字。支持多关键字与关系检索，用 $ 隔开; 至少存在一个, 用 | 分隔
		* startime：时间起始时间戳, 秒
		* endtime：时间截止时间戳, 秒
		* filter：过滤词, 多个过滤词用 | 分隔, and关系用 $
		* appkey：识别调用方
		* interval：时间间隔，1d代表一天，1m代表一个月，1h代表一小时，1w代表一个星期，默认为1d
	* 请求示例：
		* http://internal.api.durian.datatub.com/durian/api/v2/statuses/search/count?q=小米$雷军|格力$董明珠&starttime=1468341900&endtime=1468834512&filter=股票|美女&appkey=support&interval=1d
	* 返回值：Map<String, Object>

			{
				"success": boolean, true为正常返回，false为异常
				"total_number": 满足条件的记录数，不设置interval时，返回类型为int；多个区间时，返回类型为map，格式为｛time: totalNum｝
				"msg": string, 当success=false时返回的异常信息(注：异常信息未做详细处理)
			}


4. 根据uids获取对应时间段内的用户微博

	* 请求：		GET		/durian/api/status/timeline_batch
	* 接收参数：
		* uids：微博uid。最多10个uid，用逗号分隔
		* startime：时间起始时间戳, 秒
		* endtime：时间截止时间戳, 秒
		* appkey：识别调用方
	* 请求示例：
		* http://internal.api.durian.datatub.com/durian/api/status/timeline_batch?uids=1750070171,1815070622&starttime=1468341900&endtime=1468834512&appkey=cube
	* 返回值：Map<String, Object>

			{
				"success": boolean, true为正常返回，false为异常
				"total_number": int, 满足条件的记录数
				"statuses": list<Status>，微博内容列表，每条微博中的字段见附录;不考虑分页，最大返回1w条微博
				"msg": string, 当success=false时返回的异常信息(注：异常信息未做详细处理)
			}


5. 根据uids获取对应时间段内的用户微博总数

	* 请求：		GET		/durian/api/status/timeline_batch/counts
	* 接收参数：
		* uids：微博uid。最多10个uid，用逗号分隔
		* startime：时间起始时间戳, 秒
		* endtime：时间截止时间戳, 秒
		* appkey：识别调用方
	* 请求示例：
		* http://internal.api.durian.datatub.com/durian/api/status/timeline_batch/counts?uids=1750070171,1815070622&starttime=1468341900&endtime=1468834512&appkey=cube
	* 返回值：Map<String, Object>

			{
				"success": boolean, true为正常返回，false为异常
				"total_number": int, 满足条件的记录数
				"msg": string, 当success=false时返回的异常信息(注：异常信息未做详细处理)
			}


6. 8000W微博库提供随机拿到10000个uid的接口

	* 请求：		GET		/durian/api/users/uids
	* 接收参数：
		* m：后端执行的方法名，目前只有random，不能为空
		* count：需要随机生成的uid的数量，count小于10w，不能为空
		* appkey：识别调用方
	* 请求示例：
		* http://internal.api.durian.datatub.com/durian/api/users/uids?m=random&count=10000&appkey=cube
	* 返回值：Map<String, Object>
		
			{
				"success": boolean, true为正常返回，false为异常
				"uids": list<string>，UID列表
				"msg": string, 当success=false时返回的异常信息(注：异常信息未做详细处理)
			}


7. 根据uid获取用户信息

	* 请求：		GET		/durian/api/users/show_batch
	* 接收参数：
		* uids：uids数组，用逗号分隔，最多50个
		* appkey：识别调用方
	* 请求示例：
		* http://internal.api.durian.datatub.com/durian/api/users/show_batch?uids=3423,12434&appkey=cube
	* 返回值：Map<String, Object>

			{
				"success": boolean, true为正常返回，false为异常
				"users}: list<User>，用户列表，User格式见附录
				"msg": string, 当success=false时返回的异常信息(注：异常信息未做详细处理)
			}


### SDK接口：

#### DurianSearcher

1. getInstance

	* 功能：获取DurianSearcher单例对象
	* 是否静态：是
	* 接收参数：
		* 无
	* 返回值：
		* DurianSearcher，单例

2. getTotalHits

	* 功能：获取满足条件的微博数量
	* 是否静态：否
	* 接收参数：
		* keywords: String, 内容包含的关键词，多个以空格分隔
		* from: Long，开始时间戳
		* to: Long，结束时间戳
	* 返回值：
		* long，满足条件的微博数量

3. getTotalHits

	* 功能：获取满足条件的微博数量
	* 是否静态：否
	* 接收参数：
		* keywords: String, 检索关键字。支持多关键字与关系检索，用 $ 隔开; 至少存在一个, 用 | 分隔
		* filter: String，过滤词, 多个过滤词用 | 分隔, and关系用 $
		* from: long，开始时间戳
		* to: long，结束时间戳
	* 返回值：
		* long，满足条件的微博数量

4. getTotalHits

	* 功能：获取满足条件的以post_time聚合的微博数量
	* 是否静态：否
	* 接收参数：
		* keywords: String, 检索关键字。支持多关键字与关系检索，用 $ 隔开; 至少存在一个, 用 | 分隔
		* filter: String，过滤词, 多个过滤词用 | 分隔, and关系用 $
		* from: long，开始时间戳
		* to: long，结束时间戳
		* interval: String, 时间间隔，1d代表一天，1m代表一个月，1h代表一小时，1w代表一个星期，默认为1d
	* 返回值：
		* Map<String, Long>，map<post_time, totalCount>

5. exists

	* 功能：判断id是否存在
	* 是否静态：否
	* 接收参数：
		* id: String, es docId
	* 返回值：
		* boolean，true表示存在，false不存在

6. getUidsTotalHits

	* 功能：返回一批uid在指定时间段的数据量
	* 是否静态：否
	* 接收参数：
		* uids: String, 微博用户ID，多个以英文逗号分隔
		* from: long，开始时间戳
		* to: long，结束时间戳
	* 返回值：
		* long，满足条件的微博数量

7. getUsers

	* 功能：根据一批uid获取user信息
	* 是否静态：否
	* 接收参数：
		* uids: String, 微博用户ID，多个以英文逗号分隔
	* 返回值：
		* List<User>，微博用户信息列表

8. getUids

	* 功能：根据method获取一批uids
	* 是否静态：否
	* 接收参数：
		* method: String, 获取uid的方法，random等
		* count: int, 获取的数量
	* 返回值：
		* List<String>，获取的uid列表

9. getStatusesTimelineBatch

	* 功能：新浪uid搜索user_timeline接口，最多10个uid，不分布，直接返回所有微博
	* 是否静态：否
	* 接收参数：
		* uids: String, 微博用户ID，多个以英文逗号分隔
		* from: long，开始时间戳
		* to: long，结束时间戳 
	* 返回值：
		* List<Status>，满足条件的微博列表

10. searchStatuses

	* 功能：模拟新浪检索接口，最大只返回10000条
	* 是否静态：否
	* 接收参数：
		* keywords: String, 检索关键字。支持多关键字与关系检索，用 $ 隔开; 至少存在一个, 用 | 分隔
		* from: long，开始时间戳
		* to: long，结束时间戳
		* count: int, 返回条数, 最大10000条
	* 返回值：
		* List<Status>，满足条件的微博列表

11. searchStatuses

	* 功能：模拟新浪检索接口，按分页返回
	* 是否静态：否
	* 接收参数：
		* keywords: String, 检索关键字。支持多关键字与关系检索，用 $ 隔开; 至少存在一个, 用 | 分隔
		* from: long，开始时间戳
		* to: long，结束时间戳
		* page: int, 页码
		* perpage: int, 每页返回数量
	* 返回值：
		* List<Status>，满足条件的微博列表

#### DurianIndexWriter

1. getInstance

	* 功能：获取DurianIndexWriter单例对象
	* 是否静态：是
	* 接收参数：
		* 无
	* 返回值：
		* DurianIndexWriter，单例

2. writeIndex

	* 功能：将微博列表写入es
	* 是否静态：否
	* 接收参数：
		* statuses: List<Status>, 要写入索引的微博列表
	* 返回值：
		* 无

3. writeIndex

	* 功能：将微博写入es
	* 是否静态：否
	* 接收参数：
		* status: Status, 要写入索引的微博
	* 返回值：
		* 无

4. flush

	* 功能：数据刷进es
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

5. close

	* 功能：关闭es连接
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

#### CommonIndexWriter

1. loadCommonIndexWriter

	* 功能：根据条件获取CommonIndexWriter单例对象
	* 是否静态：是
	* 接收参数：
		* conf: Configuration, 配置
		* clusterName: String, es集群名称
		* indexName: String, 索引名称
		* indexType: String, 索引类型
	* 返回值：
		* CommonIndexWriter，单例

2. addData

	* 功能：将doc写入es
	* 是否静态：否
	* 接收参数：
		* doc: YZDoc, 要写入的文档
	* 返回值：
		* 无

3. addData

	* 功能：将doc写入es
	* 是否静态：否
	* 接收参数：
		* doc: YZDoc, 要写入的文档
		* opType: IndexRequest.opType, 操作类型
	* 返回值：
		* 无

4. updateData

	* 功能：更新指定文档
	* 是否静态：否
	* 接收参数：
		* doc: YZDoc, 要更新的文档
	* 返回值：
		* 无

5. deleteData

	* 功能：删除指定文档
	* 是否静态：否
	* 接收参数：
		* doc: YZDoc, 要删除的文档
	* 返回值：
		* 无

6. flush

	* 功能：数据刷进es
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

7. close

	* 功能：关闭es连接
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

#### PhoenixDriver

1. PhoenixDriver

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* zkQuorum: String, zk连接
		* zkPort: int, zk端口
		* hbaseZkPath: String, hbase路径
	* 返回值：
		* 无

2. PhoenixDriver

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* zkQuorum: String, zk连接
		* zkPort: int, zk端口
		* hbaseZkPath: String, hbase路径
		* properties: Properties, 其他参数
	* 返回值：
		* 无

3. batchExecute

	* 功能：执行sql
	* 是否静态：否
	* 接收参数：
		* sql: String, 要执行的sql，不带参数
		* params: Object[], 参数数组
	* 返回值：
		* int[], 执行结果

4. batchExecute

	* 功能：执行sql
	* 是否静态：否
	* 接收参数：
		* sql: String, 要执行的sql，不带参数
		* params: Collection<T[]>, 参数数组
	* 返回值：
		* int[], 执行结果

5. fillStatement

	* 功能：构造查询对象
	* 是否静态：否
	* 接收参数：
		* stmt: PreparedStatement, 查询对象
		* params: Object..., 不定长参数
	* 返回值：
		* 无

#### PhoenixFactory

1. getInstance

	* 功能：获取PhoenixFactory单例对象
	* 是否静态：是
	* 接收参数：
		* conf: Configuration, 配置
	* 返回值：
		* PhoenixFactory，单例

#### PhoenixBatchWriter

1. getInstance

	* 功能：获取PhoenixBatchWriter单例对象
	* 是否静态：是
	* 接收参数：
		* cacheSize: int, 缓存长度
	* 返回值：
		* PhoenixBatchWriter，单例

2. PhoenixBatchWriter

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* cacheSize: int, 缓存长度
	* 返回值：
		* 无

3. offerStatus

	* 功能：写入队列
	* 是否静态：否
	* 接收参数：
		* status: Status, 微博
	* 返回值：
		* 无

4. write

	* 功能：写入队列并刷新
	* 是否静态：否
	* 接收参数：
		* status: Status, 微博
	* 返回值：
		* 无

5. batchWrite

	* 功能：批量写入
	* 是否静态：否
	* 接收参数：
		* statuses: List<Status>, 要写入的微博列表
	* 返回值：
		* 无

6. flush

	* 功能：刷入
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

7. cleanup

	* 功能：刷入
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

8. close

	* 功能：关闭连接
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无


#### HBaseDataClient

1. HBaseDataClient

	* 功能：构造函数
	* 是否静态：否
	* 接收参数：
		* 无
	* 返回值：
		* 无

2. getUser

	* 功能：根据uid获取用户
	* 是否静态：否
	* 接收参数：
		* uid
	* 返回值：
		* User, 用户信息

3. getStatusFromHBase

	* 功能：从hbase获取微博
	* 是否静态：否
	* 接收参数：
		* mid: String，微博MID
		* uid: String, 微博用户ID
	* 返回值：
		* Status, 根据mid和uid取出的微博

4. getStatusFromHBase

	* 功能：批量写入
	* 是否静态：否
	* 接收参数：
		* mid: String, 微博MID
		* uid: String, 微博用户ID
		* retweetMid: String, 回复微博MID
		* retweetUid: String, 回复微博用户ID
	* 返回值：
		* Status, 根据mid和uid取出的微博，并将retweetMid和retweetUid获得的微博设置到回复中

5. batchGetStatuses

	* 功能：使用es查询回来meta信息, 批量查询hbase拿回原始数据
	* 是否静态：否
	* 接收参数：
		* metaData: List<Map<String, Object>, es查询回来的meta信息
	* 返回值：
		* List<Status>, 查询到的微博列表

### 附录

	/* Status内容格式 */
	[
		{
			"id": string, id
			"mid": string, mid
			"text": string, 微博内容
			"source": Source, 微博发送终端, Source格式见下文
			"geo": string, 地理位置信息
			"repostsCount": int, 转发数
			"commentsCount": int, 评论数
			"attitudesCount": int, 点赞数
			"uid": string, 微博用户id
			"createdAt": Date, 微博发布时间
			"picUrls": list<string>, 微博配图的url
			"originalPic": string, 原始图片地址
			"annotations": string, 是否转发(0-转发 1-原创)
			"feature": int, 元数据,关于这条微博的结构化信息
			"user": User, 微博用户, User格式见下文
			"retweetedStatus": Status, 转发的微博
			"idstr": long, 默认值0
			"favorited": boolean, 默认值false
			"truncated": boolean, 默认值false
			"inReplyToStatusId": long, 默认值0
			"inReplyToUserId": long, 默认值0
			"latitude": double, 默认值－1
			"longitude": double, 默认值－1
			"mlevel": int, 默认值0
		},
		...
	]

	/* Source格式 */
	{
		"url": string
		"name": string, 终端名称
	}

	/* User格式 */
	{
		"id": string, 微博用户id
		"verifiedType": int, 用户认证类型
		"name": string, 用户名称
		"province": int, 省份代码
		"city": int, 城市代码
		"description": string, 用户描述
		"profileImageUrl": string, 用户头像地址
		"url": string, 用户博客地址
		"gender": string, 用户性别
		"followersCount": int, 粉丝数
		"friendsCount": int, 关注数
		"statusesCount": int, 微博数
		"favouritesCount": int, 收藏数
		"createdAt": Date, 用户注册时间
		"location": string, 用户所在地
		"domain": string, 用户的个性化域名
		"weihao": string, 用户的微号
		"biFollowersCount": int, 相互关注数
		"following": boolean, 默认值false
		"verified": boolean, 默认值false
		"allowAllActMsg": boolean, 默认值false
		"allowAllComment": boolean, 默认值false
		"followMe": boolean, 默认值false
		"onlineStatus": int, 默认值0
	}
