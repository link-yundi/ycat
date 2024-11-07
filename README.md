# ycat
本地文件数据库，支持所有格式

### 安装
```shell
pip install -U git+https://github.com/link-yundi/ycat.git
```

### 使用示例
```python
import ycat

data = [1, 2, 3]
table_name = 'a/b/c/test'

# 存储数据->catdb/a/b/c/test
ycat.put(data, table_name)
# 读取
ycat.get(table_name)
# 删除
ycat.delete(table_name)
# 是否存在某张表
ycat.has(table_name)

# 默认db: {pwd}/catdb
# 改变数据库
ycat.connect('/your/db/path')
```