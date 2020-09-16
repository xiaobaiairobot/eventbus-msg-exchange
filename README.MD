# 云粒智慧数据总线消息转换范例

开发目的：为了辅助南部新城项目中，将CSV格式数据转换成JSON结构。

运行前提：数据总线中有2个Topic，编码分别为：topicA,topicB，其中topicA为设备发过来的csv结构数据，用本程序监听topicA，然后将数据转换为json结构发送给topicB，其他业务系统消费topicB中的数据以便获取json格式的数据。

## 打包编译方法
```shell script
# mvn clean package -DskipTests
# ./build-images.sh demo
# ./push-images.sh demo
```

## 启动
```shell script
# docker run -p 50002:50002 -d --restart always --name eventbus_demo registry.cn-beijing.aliyuncs.com/yunli-bigdata/eventbus-msg-exchange:1.0.0
```

## 发送
参数中server和producerPort可不配，使用系统配置文件的即可，参数有值会优先使用参数中的
如果模拟多个column，使用英文逗号分隔即可，数据会自动随机生成，如：int,string,long,double,string这样即发送5列的数据
sendTimes表示一次请求发送多少条数据（连续发送中间不停顿）
```shell script
# curl --location --request POST 'http://172.30.13.177:50002/eb/test' \
  --header 'Content-Type: application/json' \
  --data-raw '{
  	"server":"172.30.13.177",
  	"producerPort":"31236",
  	"sourceTopic":"autotestSQ",
  	"sendTimes":100,
  	"columns":"string"
  }'
```
带证书方式的提交
```shell script
# curl --location --request POST 'http://localhost:50002/eb/test/credential' \
  --header 'Content-Type: application/json' \
  --data-raw '{
  	"server":"172.30.13.177",
  	"producerPort":"32236",
  	"sourceTopic":"a52b4029d0f24c909b019c39e96df9d5",
  	"sendTimes":2,
  	"columns":"name,sex,age",
  	"key": "bigdata#system",
          "signature": "b649*******cKrE=",
          "expiredDate": "2020-08-27 13:00:00.000",
          "appId": "bigdata_dsep",
          "privileges": [
              {
                  "resource": "a52b4029d0f24c909b019c39e96df9d5",
                  "action": "consume"
              },
              {
                  "resource": "a52b4029d0f24c909b019c39e96df9d5",
                  "action": "consume"
              },
              {
                  "resource": "a52b4029d0f24c909b019c39e96df9d5",
                  "action": "produce"
              }
          ]
  }'
```