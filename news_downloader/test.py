from bson import ObjectId
import ezpymongo
import lzma

db = ezpymongo.Connection()
one = db.get_one({'_id': ObjectId('62e835aaa25579f0843a61f4')})
print(one)
# print(lzma.decompress(bytes(one['url'], encoding='utf-8')))
