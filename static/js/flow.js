define(["comp/treeview", "comp/flowCanvas", "comp/flowInspector", "util","comp/showview","comp/queryview"], function(TreeView, FlowCanvas, FlowInspector, Util,ShowView, QueryView) {
    var Flow = {};
    var canvas = undefined;

    Flow.render = function() {
        $("#mainUI").empty();
        // 根节点
        var rootUI = d3.select("#mainUI").append("div").classed("row", true);
        // 左边节点树
        var nodeTree = rootUI.append("div").classed("col-md-2", true).attr("id", "flowTree");

        // 中间面板
        var flowUI = rootUI.append("div").classed("col-md-5", true).attr("id", "flowUI");
        var flowCanvas = flowUI.append("div").classed("row", true).attr("id", "flowCanvas");
        var flowInspector = flowUI.append("div").classed("row", true).attr("id", "flowInspector");


        // 右边结果输出窗口
        var showUI = rootUI.append("div").classed("col-md-3", true).attr("id", "showUI");
        var headCanvas = showUI.append("div").classed("row", true).attr("id", "headCanvas");
        var listCanvas = showUI.append("div").classed("row", true).attr("id", "listCanvas");

        var queryUI = rootUI.append("div").classed("col-md-3", true).attr("id", "queryUI");
        var queryCanvas = queryUI.append("div").classed("row", true).attr("id", "queryCanvas");


        // Init Tree 
        $.get("/nodes", function(data) {
            var nodeTreeSpecification = data;
            var treeView = new TreeView("flowTree", nodeTreeSpecification)
            treeView.render();


            var resView = new ShowView("headCanvas");
            resView.render();

            var resView1 = new QueryView("queryCanvas");
            resView1.render();


//             var res2View = new ShowView("Query");
//             res2View.render();

            

            // Init Flow Inspector
            var inspector = new FlowInspector("flowInspector");
            inspector.render();

            canvas = new FlowCanvas("flowCanvas", nodeTreeSpecification, inspector );
            canvas.render();
        }
        );
    }

    return Flow;
});
