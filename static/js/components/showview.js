define(["util"], function(Util) {
    var ShowView = function(rootId) {
        this._rootId = rootId;
   
    };

    ShowView.prototype.render = function() {
        var root = d3.select("#" + this._rootId);
        //var panel = Util.addPanel(root, "result");
        var panel = Util.addPanel(root,"result");
        var p2 = panel.select(".panel-body").append("div").attr("id", "colunmslist");
        //var p2 = panel.select(".panel-body").append("div").attr("id");
        var h1 = p2.append("table").classed('table table-bordered table-condensed',true).attr("id", "table2").append('tr');
        //var h1 = p2.append("table").classed('table table-bordered table-condensed',true).attr("id").append('tr');
        h1.append("th").text("id").style("width", "22%");
        h1.append("td").text("value");
        
    };

    return ShowView;
});
