server.py used to deploy the server environment, click to run it.
Input  http://127.0.0.1:5000/# in browser, enter graphic user interface of workflow-based data wrangling tool, shown in GUI.png

In flow panel:

The left panel displays a set of data wranglings tool, the middle panel is regarded as operating area.
Drag the selected operations into operating area as visual nodes which generate the inspector information below the operating panel, so that 
users can modify the parameters of nodes.

The operations for workflows are listed in the head of middle panel, for: create workflow, load previously saved workflow, save workflow,delete workflow,
search workflow descriptions in term of json.

When user clicks the  run button on the inspector panel of data wrangling operation, the result of nodes in front of it and this node will be returned
in the result panel. When the run node is the final one in a workflow, it means that the result of entire workflow will be generated as well.

There are four data wrangling workflows which are saved in load panel.


In node panel:

The codes of all the data wrangling operations are displayed here, which corresponds to python functions, shown like data wrangling operations.png.

Actually, these codes for data wrangling operations are stored in repo.db as wll.



