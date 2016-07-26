# File Post Action


Description
-----------
Apply action (Delete, Move or Archive) on file(s) at the end of pipeline.


Use Case
--------
This action can be used when you want to apply action (Delete, Move or Archive) on file(s) at the end of pipeline.
For example, you may want to configure a pipeline so that a file get deleted after completion of pipeline.


Properties
----------
**runCondition:**" When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'completion'.
If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or failed.
If set to 'success', the action will only be executed if the pipeline run succeeded.
If set to 'failure', the action will only be executed if the pipeline run failed.

**path:** Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'.

**action:** Action to be taken on the file(s).
Possible actions are -
1. None - no action required.
2. Delete - delete from the HDFS.
3 Archive - archive to the target location.
4. Moved - move to the target location.

**targetFolder:** Target folder path if user select action as either ARCHIVE or MOVE.
Target folder must be an existing directory.

**subject:** The subject of the email.

**pattern:** Pattern to select specific file(s)." +
Example -
1. Use '^' to select file with name start with 'catalog', like '^catalog'.
2. Use '$' to select file with name end with 'catalog.xml', like 'catalog.xml$'.
3. Use '*' to select file with name contains 'catalogBook', like 'catalogBook*'.

Example
-------
This example move file(s) which are matching the pattern, from the path specified to the target folder whenever a run
fails:

    {
        "name": "FileAction",
        "type": "postaction",
        "properties": {
            "path": "/opt/hdfs/source/",
            "action": "Move",
            "targetFolder": "/opt/hdfs/target/",
            "pattern": "^catalog",
            "runCondition": "failure"
        }
    }
