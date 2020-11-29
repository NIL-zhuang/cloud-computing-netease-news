import os
import json
import time
import shutil


def isDigit(word: str):
    try:
        return isinstance(int(word), int)
    except ValueError:
        return False


def get_count(filename):
    with open(filename) as f:
        lines = f.readlines()
        if len(lines) == 0:
            return None
    words = [eval(line) for line in lines]
    words = [word for word in words if not isDigit(word[0])]
    words = sorted(words, key=lambda x: x[1], reverse=True)[:20]
    res = list()
    for word in words:
        t = dict()
        t["关键词"] = word[0]
        t["数量"] = word[1]
        res.append(t)
    return res


if __name__ == "__main__":
    # 小时，词，关键词
    item_list = []
    cur_time = time.strftime("%Y-%m-%d %H:%M", time.localtime())
    for dirs in os.listdir("./wct"):
        # dir是时间戳 todo 修改
        # cur_time = '-'.join(dirs.split('-')[:-1])
        data = get_count("./wct/"+dirs+"/part-00000")
        if data is None:
            shutil.rmtree("./wct/"+dirs)
            continue
        cur_json = {cur_time: data}
        print(cur_json)
        fp = open("./wordcount_history.json")
        model = json.load(fp)
        fp.close()
        for i in cur_json:
            model[i] = cur_json[i]
        jsObj = json.dumps(model, ensure_ascii=False)
        with open("./wordcount_history.json", "w", encoding='utf-8') as fw:
            fw.write(jsObj)
            fw.close()
        shutil.rmtree("./wct/"+dirs)
