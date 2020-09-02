import json
import pandas as pd
import sys
from datetime import date as date_


class Engine:

    def date(self, datestr):
        parts = datestr.split(' ')
        return parts[0]

    def time(self, datestr):
        parts = datestr.split(' ')
        return parts[1]

    def dayofweek(self, datestr):
        parts = datestr.split('/')
        ans = date_(int(parts[0]), int(parts[1]), int(parts[2])).weekday()
        return ans

    def runner(self, file):
        data_frames = {}
        operators = {}
        with open(file, "r") as read_file:
            data = json.load(read_file)
        functions = self.resolve_imports(data)
        functions["date"] = self.date
        functions["time"] = self.time
        functions["dayofweek"] = self.dayofweek
        functions["avg"] = self.avg
        functions["jt"] = self.jt
        functions["enrich_timestamp"] = self.enrich_timestamp
        grouping_function = None
        sequence = data["sequence"]
        for operator in data["operators"]:
            operators[operator["id"]] = operator
        for i in sequence:
            operator = operators[i]
            name = operator["operator"]
            if name == "read":
                self.read(operator, data_frames)
            elif name == "select":
                self.select(operator, data_frames, functions)
            elif name == "wrangling":
                function_name = operator["function_name"]
                # print(function_name)
                # print(functions)
                self.wrangling(operator, data_frames, functions)
            elif name == "union":
                self.union(operator, data_frames)
            elif name == "join":
                self.join(operator, data_frames)
            elif name == "project":
                self.project(operator, data_frames, functions)
            elif name == "write":
                self.write(operator, data_frames, functions)
            elif name == "spatial_temporal_join":
                self.spatial_temporal_join(operator, data_frames)
            elif name == "group_by":
                grouping_function = self.group_by(operator, data_frames)

    def resolve_imports(self, data):
        imports = data["imports"]
        functions = {}
        for imp in imports:
            name = imp.lower()
            module = __import__(name)
            func = getattr(module, name)
            functions[name] = func
        return functions

    def read(self, read_op, frames):
        filename = read_op["file"]["name"]
        alias = read_op["file"]["alias"]

        path = filename
        #print(path)
        frame = None
        if ".csv" in filename:
            frame = pd.read_csv(path)
        elif ".json" in filename:
            with open(path, 'r') as f:
                frame = json.load(f)
        frames[alias] = frame

    def select(self, select_op, frames, functions):
        file = select_op["file"]
        conditions = select_op["conditions"]
        for condition in conditions:
            frame = frames[file]
            if condition["type"] == "simple":
                if condition["left"]["type"] == "column" and condition["right"]["type"] == "literal":
                    column = condition["left"]["value"]["column"].replace("_", " ")
                    literal = condition["right"]["value"].replace("'", "")
                    if condition["comparison"] == "=":
                        new_frame = None
                        if literal.isdigit():
                            new_frame = frame[frame[column] == int(literal)]
                        else:
                            new_frame = frame[frame[column] == literal]
                        frames[file] = new_frame
                elif condition["left"]["type"] == "function" and condition["right"]["type"] == "literal":
                    first_function = condition["left"]["value"]["name"].lower()
                    second_function = None
                    argument = None
                    if condition["left"]["value"]["arguments"][0]["type"] == "function":
                        second_function = condition["left"]["value"]["arguments"][0]["value"]["name"].lower()
                        argument = condition["left"]["value"]["arguments"][0]["value"]["arguments"][0]["value"][
                            "column"]
                    else:
                        argument = condition["left"]["value"]["arguments"][0]["value"]["column"]
                    literal = condition["right"]["value"].replace("'", "")
                    column = argument
                    if second_function is None:
                        new_frame = None
                        if condition["comparison"] == "=":
                            new_frame = frame[
                                frame[column].map(lambda a: (str(functions[first_function](a)) == literal))]
                        elif condition["comparison"] == "<":
                            new_frame = frame[
                                frame[column].map(lambda a: (str(functions[first_function](a)) < literal))]
                        elif condition["comparison"] == ">":
                            new_frame = frame[
                                frame[column].map(lambda a: (str(functions[first_function](a)) > literal))]
                        elif condition["comparison"] == "<=":
                            new_frame = frame[
                                frame[column].map(lambda a: (str(functions[first_function](a)) <= literal))]
                        elif condition["comparison"] == ">=":
                            new_frame = frame[
                                frame[column].map(lambda a: (str(functions[first_function](a)) >= literal))]
                        frames[file] = new_frame
                    else:
                        new_frame = None
                        if condition["comparison"] == "=":
                            new_frame = frame[frame[column].map(
                                lambda a: (str(functions[first_function](functions[second_function](a))) == literal))]
                        elif condition["comparison"] == "<":
                            new_frame = frame[frame[column].map(
                                lambda a: (str(functions[first_function](functions[second_function](a))) < literal))]
                        elif condition["comparison"] == ">":
                            new_frame = frame[frame[column].map(
                                lambda a: (str(functions[first_function](functions[second_function](a))) > literal))]
                        elif condition["comparison"] == "<=":
                            new_frame = frame[frame[column].map(
                                lambda a: (str(functions[first_function](functions[second_function](a))) <= literal))]
                        elif condition["comparison"] == ">=":
                            new_frame = frame[frame[column].map(
                                lambda a: (str(functions[first_function](functions[second_function](a))) >= literal))]
                        frames[file] = new_frame

    def enrich_timestamp(self, operator, frames):
        file = operator["arguments"][0]["value"]["file"]
        column = operator["arguments"][0]["value"]["column"]
        frame = frames[file]
        pattern = operator["arguments"][1]["value"]
        start = 0
        if len(operator["arguments"]) >= 3:
            start = operator["arguments"][2]["value"]
        separator_index = pattern.index("d")
        separator = pattern[separator_index + 1]
        if start == 0:
            self.enhanced_enrich(operator, frames)
            return

        frame[column] = frame[column].map(lambda a: self.enrich(a, separator))

    def union(self, operator, frames):
        first = operator["first"]
        second = operator["second"]
        file1 = frames[first]
        file2 = frames[second]
        files = [file1, file2]
        new_frame = pd.concat(files)
        frames[first] = new_frame
        frames[second] = new_frame

    def join(self, operator, frames):
        files = operator["files"]
        file_frames = []
        if frames[files[0]] is frames[files[1]]:
            conditions = operator["key"]["conditions"]
            results = []
            for condition in conditions:
                file1 = condition["left"]["value"]["file"]
                column1 = condition["left"]["value"]["column"].replace("_", " ")
                file2 = condition["right"]["value"]["file"]
                column2 = condition["right"]["value"]["column"].replace("_", " ")
                frame1 = frames[file1]
                frame2 = frames[file2]
                # frame2.rename(columns={column2:column1}, inplace=True)
                frame1[column1] = frame1[column1].map(lambda a: self.last_4(a))
                frame1[column1] = frame1[column1].astype("int64")
                result = frame1.merge(frame2, left_on=column1, right_on=column2)
                results.append(result)
            res = pd.concat(results)
            for file in files:
                frames[file] = res

    def spatial_temporal_join(self, operator, frames):
        files = operator["files"]
        file_frames = []
        conditions = operator["conditions"]
        # needs completion

    def group_by(self, operator, frames):
        grouping_function = operator["function"]
        return grouping_function

    def project(self, operator, frames, functions):
        columns = operator["columns"]
        new_cols = []
        for col in columns:
            col = col.replace("_", " ")
            parts = col.split(" ")
            if len(parts) == 2:
                new_col = col.split(" ")[0] + " (" + col.split(" ")[1] + ")"
                new_cols.append(new_col)
            else:
                new_cols.append(col)
        itr = iter(frames.values())
        itr.__next__()
        frame = itr.__next__()
        # frame = frame.loc[:, new_cols]
        frame = frame.reindex(columns=new_cols)

        for k, v in frames.items():
            # print(type(v))
            # frame = v.loc[ new_cols]
            frames[k] = frame

    def write(self, operator, frames, functions):
        # itr = iter(frames.values())
        # v = list(frames.values())
        # print(itr)
        # frame = v[0]
        # print(frame)
        function = operator["function"]["name"].lower()
        ##print(function)
        arguments = operator["function"]["arguments"]
        args = []
        final_result=0
        for frame in frames.values():
            for arg in arguments:
                if arg["type"] == "literal":
                    val = arg["value"]
                    num = self.avg(val, frame)
                    args.append(num)
                else:
                    inner_function = arg["value"]["name"].lower()
                    inner_arg_raw = arg["value"]["arguments"][0]["value"].replace("_", " ")
                    inner_arg = inner_arg_raw.split(" ")[0] + " (" + inner_arg_raw.split(" ")[1] + ")"
                    res = functions[inner_function](inner_arg, frame)
                    args.append(res)
        if len(args) >=1:
            final_result = functions[function](args[0], args[1])
        #print(final_result)
        res = round(final_result, 2)
        file_name = "output/"+operator["file"]
        d = {res}
        #print(d)
        frame = pd.DataFrame(d, columns=["Journey Time (hrs)"])
        frame.to_csv(file_name)

    def wrangling(self, operator, frames, functions):
        function_name = operator["function_name"].lower()
        functions[function_name](operator, frames)

    def enrich(self, str, separator, additional=False, to_add=""):
        if additional:
            str.replace("-", separator)
            str = str[:-1]
            str = str + " " + to_add
            return str
        else:
            str = str.replace("-", separator)
            str = str.split(".")[0]
            return str

    def enhanced_enrich(self, operator, frames):
        # for future use
        file = operator["arguments"][0]["value"]["file"]
        column = operator["arguments"][0]["value"]["column"]
        frame = frames[file]
        pattern = operator["arguments"][1]["value"]
        start = 0
        if len(operator["arguments"]) >= 3:
            start = operator["arguments"][2]["value"]
        separator_index = pattern.index("d")
        separator = pattern[separator_index + 1]
        offset = frame["$"]
        for index, row in frame.iterrows():
            row[column] = self.enrich(row[column], separator, True, str(int(row["$"]) / 60))

    def last_4(self, arg):
        string = str(arg)
        string = string[-4:]
        return string

    def avg(self, col, frame):
        ans = frame.loc[:, col].mean()
        return ans

    def jt(self, len, avg_speed):
        return float(len) / avg_speed

if __name__ == "__main__":
    eng = Engine()
    ##st='{"query":"query2.json","imports":[],"sequence":[0,0,0,0,0,0,0,0,0,0,0,0,0,0],"operators":[{"id":0,"operator":"read","file":{"name":"rawpvr_2018_02_01_28d_1415.csv","alias":"f2","type":"csv"},"dependencies":[]},{"id":0,"operator":"read","file":{"name":"rawpvr_2018_02_01_28d_1083.csv","alias":"f1","type":"csv"},"dependencies":[]},{"id":0,"operator":"select","file":"f2","conditions":[{"type":"simple","left":{"type":"column","value":{"file":"f2","column":"Direction_Name"}},"right":{"type":"literal","value":"'NorthEast'"},"comparison":"="}],"dependencies":[0]},{"id":0,"operator":"select","file":"f1","conditions":[{"type":"simple","left":{"type":"column","value":{"file":"f1","column":"Direction_Name"}},"right":{"type":"literal","value":"'North'"},"comparison":"="}],"dependencies":[0]},{"id":0,"operator":"wrangling","type":"function_call","function_name":"ENRICH_TIMESTAMP","arguments":[{"type":"column","value":{"file":"f2","column":"Date"}},{"type":"literal","value":"'%d/%c/%Y %H:%i:%s'"},{"type":"literal","value":"\"01/02/2018\\\""}],"dependencies":[0]},{"id":0,"operator":"wrangling","type":"function_call","function_name":"ENRICH_TIMESTAMP","arguments":[{"type":"column","value":{"file":"f1","column":"Date"}},{"type":"literal","value":"'%d/%c/%Y %H:%i:%s'"},{"type":"literal","value":"\"01/02/2018\\\""}],"dependencies":[0]},{"id":0,"operator":"read","file":{"name":"staticSitesInfo.csv","alias":"f3","type":"csv"},"dependencies":[]},{"id":0,"operator":"select","file":"f2","conditions":[{"type":"simple","left":{"type":"function","value":{"name":"DAYOFWEEK","arguments":[{"type":"function","value":{"name":"DATE","arguments":[{"type":"column","value":{"file":"f2","column":"Date"}}]}}]}},"right":{"type":"literal","value":"'4'"},"comparison":"="},{"type":"simple","left":{"type":"function","value":{"name":"TIME","arguments":[{"type":"column","value":{"file":"f2","column":"Date"}}]}},"right":{"type":"literal","value":"'17:00:00'"},"comparison":">="},{"type":"simple","left":{"type":"function","value":{"name":"TIME","arguments":[{"type":"column","value":{"file":"f2","column":"Date"}}]}},"right":{"type":"literal","value":"'18:00:00'"},"comparison":"<"}],"dependencies":[0]},{"id":0,"operator":"select","file":"f1","conditions":[{"type":"simple","left":{"type":"function","value":{"name":"DAYOFWEEK","arguments":[{"type":"function","value":{"name":"DATE","arguments":[{"type":"column","value":{"file":"f1","column":"Date"}}]}}]}},"right":{"type":"literal","value":"'4'"},"comparison":"="},{"type":"simple","left":{"type":"function","value":{"name":"TIME","arguments":[{"type":"column","value":{"file":"f1","column":"Date"}}]}},"right":{"type":"literal","value":"'17:00:00'"},"comparison":">="},{"type":"simple","left":{"type":"function","value":{"name":"TIME","arguments":[{"type":"column","value":{"file":"f1","column":"Date"}}]}},"right":{"type":"literal","value":"'18:00:00'"},"comparison":"<"}],"dependencies":[0]},{"id":0,"operator":"select","file":"f3","conditions":[{"type":"simple","left":{"type":"column","value":{"file":"f3","column":"StartSite"}},"right":{"type":"literal","value":"'1083'"},"comparison":"="},{"type":"simple","left":{"type":"column","value":{"file":"f3","column":"EndSite"}},"right":{"type":"literal","value":"'1415'"},"comparison":"="}],"dependencies":[0]},{"id":0,"operator":"union","first":"f1","second":"f2","dependencies":[0,0]},{"id":0,"operator":"join","files":["f1","f2","f3"],"key":{"type":"disjunction","conditions":[{"type":"simple","left":{"type":"column","value":{"file":"f1","column":"Site_ID"}},"right":{"type":"column","value":{"file":"f3","column":"StartSite"}},"comparison":"="},{"type":"simple","left":{"type":"column","value":{"file":"f2","column":"Site_ID"}},"right":{"type":"column","value":{"file":"f3","column":"EndSite"}},"comparison":"="}]},"dependencies":[0,0]},{"id":0,"operator":"project","file":"","columns":["LinkLength","Speed_mph"],"dependencies":[0]},{"id":0,"operator":"write","function":{"name":"JT","arguments":[{"type":"literal","value":"LinkLength"},{"type":"function","value":{"name":"AVG","arguments":[{"type":"literal","value":"Speed_mph"}]}}]},"file":"example_1_results.csv","dependencies":[0]}]}'
    #eng.runner("output/query_1598640721252.json")
    #eng.runner("output/query_1598729083159.json")
