require(["flow", "node", "model/repo", "util"], function(Flow, Node, Repo, Util) {

    jsPlumb.ready(function() {
        console.log("jsPlumb is ready to use");

        $.fn.editable.defaults.mode = 'inline';

        $("#menuFlow").click(function() {
            toggleMenu($(this));
            Flow.render();
        });

        $("#menuNode").click(function() {
            toggleMenu($(this));
            Node.render();
        });


        $("#dump-action-menu").click(function() {
            dumpRepo();
        });

        Flow.render();
    });

    function toggleMenu(me) {
        if (!me.parent().hasClass("active")) {
            $("#menuNode").parent().toggleClass("active");
            $("#menuFlow").parent().toggleClass("active");
        }
    }


});