using System;
using System.IO;
using System.Text;
using System.Data.SqlClient;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Wrapper = Microsoft.SqlServer.Dts.Runtime.Wrapper;

namespace FileLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "Data Migrator";
            Console.ForegroundColor = ConsoleColor.Yellow;

            // Read input parameters
            var sourceConnStr = args[0];
            var targetConnStr = args[1];
            var tableName = args[2];
            var outputPath = args[3];

            Console.WriteLine(tableName);
            Console.WriteLine("Building Package...");

            // Create a new SSIS Package
            var package = new Package();

            // Add Connection Managers to the Package or appropriate type
            var sourceConnection = package.Connections.Add("OLEDB");
            var targetConnection = package.Connections.Add("OLEDB");

            sourceConnection.ConnectionString = sourceConnStr.ToString();
            sourceConnection.Name = "Source DB Connection";
            sourceConnection.Description = "Source DB connection";

            targetConnection.ConnectionString = targetConnStr.ToString();
            targetConnection.Name = "Target DB Connection";
            targetConnection.Description = "Target DB connection";

            // Add a Data Flow Task to the Package
            var e = package.Executables.Add("STOCK:PipelineTask");
            var mainPipe = e as TaskHost;

            if (mainPipe != null)
            {
                mainPipe.Name = "MyDataFlowTask";
                var dataFlowTask = mainPipe.InnerObject as MainPipe;

                var app = new Application();

                if (dataFlowTask != null)
                {

                    // Add an OLE DB Destination Component to the Data Flow for the source
                    var sourceComponent = dataFlowTask.ComponentMetaDataCollection.New();
                    sourceComponent.Name = "MyOLEDBSource";
                    sourceComponent.ComponentClassID = app.PipelineComponentInfos["OLE DB Source"].CreationName;

                    // Get the design time instance of the source Ole Db Destination component
                    var sourceInstance = sourceComponent.Instantiate();
                    sourceInstance.ProvideComponentProperties();

                    // Set the source Ole Db Destination Connection
                    sourceComponent.RuntimeConnectionCollection[0].ConnectionManagerID = sourceConnection.ID;
                    sourceComponent.RuntimeConnectionCollection[0].ConnectionManager =
                        DtsConvert.GetExtendedInterface(sourceConnection);

                    // Set source destination load type
                    sourceInstance.SetComponentProperty("AccessMode", 0); // set to 0 due to primary source is DB2, otherwise use 3

                    // Add an OLE DB Destination Component to the Data Flow for the target
                    var targetComponent = dataFlowTask.ComponentMetaDataCollection.New();
                    targetComponent.Name = "MyOLEDBDestination";
                    targetComponent.ComponentClassID = app.PipelineComponentInfos["OLE DB Destination"].CreationName;

                    // Get the design time instance of the target Ole Db Destination component
                    var targetInstance = targetComponent.Instantiate();
                    targetInstance.ProvideComponentProperties();

                    // Set the target Ole Db Destination Connection
                    targetComponent.RuntimeConnectionCollection[0].ConnectionManagerID = targetConnection.ID;
                    targetComponent.RuntimeConnectionCollection[0].ConnectionManager =
                        DtsConvert.GetExtendedInterface(targetConnection);

                    // Set target destination load type
                    targetInstance.SetComponentProperty("AccessMode", 3);

                    // Now set Ole Db Destination Table names
                    sourceInstance.SetComponentProperty("OpenRowset", tableName);
                    targetInstance.SetComponentProperty("OpenRowset", tableName);

                    // Reinitialize the metadata
                    sourceInstance.AcquireConnections(null);
                    sourceInstance.ReinitializeMetaData();
                    sourceInstance.ReleaseConnections();
                    targetInstance.AcquireConnections(null);
                    targetInstance.ReinitializeMetaData();
                    targetInstance.ReleaseConnections();

                    // Create a Precedence Constraint between source and target Components
                    var path = dataFlowTask.PathCollection.New();
                    path.AttachPathAndPropagateNotifications(sourceComponent.OutputCollection[0],
                        targetComponent.InputCollection[0]);

                    // Get the list of available columns
                    var targetInput = targetComponent.InputCollection[0];
                    var targetvInput = targetInput.GetVirtualInput();

                    var targetVirtualInputColumns =
                        targetvInput.VirtualInputColumnCollection;

                    // Map Flat File Source Component Output Columns to Ole Db Destination Input Columns
                    foreach (IDTSVirtualInputColumn100 vColumn in targetVirtualInputColumns)
                    {
                        var inputColumn = targetInstance.SetUsageType(targetInput.ID,
                            targetvInput, vColumn.LineageID, DTSUsageType.UT_READONLY);

                        var externalColumn =
                            targetInput.ExternalMetadataColumnCollection[inputColumn.Name];

                        targetInstance.MapInputColumn(targetInput.ID, inputColumn.ID, externalColumn.ID);
                    }
                }
                Console.WriteLine("Executing Package...");
                package.Execute();

                var dtsx = new StringBuilder();
                dtsx.Append(outputPath).Append("\\").Append(tableName).Append(".dtsx");

                if( File.Exists( dtsx.ToString() ))
                {
                    File.Delete(dtsx.ToString());
                }

                Console.WriteLine("Saving Package...");
                app.SaveToXml(dtsx.ToString(), package, null);
            }

            package.Dispose();
            Console.WriteLine("Done");

            Console.ReadLine();
        }
    }
}