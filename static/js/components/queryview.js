define(["util"], function(Util) {
    var QueryView = function(rootId) {
        this._rootId = rootId;
    };

    QueryView.prototype.render = function() {
        var root = d3.select("#" + this._rootId);
        var panel = Util.addPanel(root,"Query");
        var p2 = panel.select(".panel-body").append("div").attr("id", "colunmslist")
        //var p2 = panel.select(".panel-body").append("div").attr("id");
        p2.html(' <form action = "/uploader" method = "POST" enctype = "multipart/form-data"><table class="table table-bordered table-condensed" id="table4"><tbody><tr><th style="width: 22%;">input</th><td><input id="file" type="file" name="file"></td></tr><tr><td style="width: 22%;text-align:center;" colspan="2"><input type="submit" class="btn btn-xs btn-primary" value="Run" /></td></tr></tbody></table></form>')
        //p2.html(' <form action = "/uploader" method = "POST" enctype = "multipart/form-data"><table class="table table-bordered table-condensed" id="table4"><tbody><tr><th style="width: 22%;">input</th><td><input id="file" type="file" name="file"></td></tr><tr><th style="width: 22%;">Output</th><td><textarea id="text_query" readonly="true" name="text_query" rows="5" cols="30"></textarea></td></tr><tr><td style="width: 22%;text-align:center;" colspan="2"><input type="submit" class="btn btn-xs btn-primary" value="Runing" /></td></tr></tbody></table></form>')
        //var h1 = p2.append("table").classed('table table-bordered table-condensed',true).attr("id", "table3").append('tr');
        //var h1 = p2.append("table").classed('table table-bordered table-condensed',true).attr("id").append('tr');
//        h1.append("td").text("Query233").style("width", "22%");
//        h1.append("td").append("input").attr("id", "query_text").attr("name", "query_text");
//        h1.append("tr");
//        h1.append("td").text("Output").style("width", "22%");
//        h1.append("td").append("textarea").attr("id", "text_query").attr("name", "text_query").attr("readonly", "true").attr("rows", "5").attr("cols", "30").append("tr");
//        h1.append("td").text("Action").style("width", "22%").style("text-align", "center%").attr("colspan", "2");
    };

    return QueryView;
});
