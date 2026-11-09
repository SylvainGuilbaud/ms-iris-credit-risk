/// DO NOT EDIT this class. Generated using CreditRiskPyProduction python class
Class dc.creditrisk.pyprod.CreditRiskPyProduction Extends Ens.Production
{

XData ProductionDefinition
{
<Production Name="dc.creditrisk.pyprod.CreditRiskPyProduction" TestingEnabled="false" LogGeneralTraceEvents="false">
  <Description> DO NOT EDIT this production from the UI. Credit risk scoring over Kafka, implemented with intersystems_pyprod</Description>
  <ActorPoolSize>2</ActorPoolSize>

    <Item Name="CreditRiskPredictService" Category="" ClassName="dc.creditrisk.pyprod.CreditRiskPredictService" PoolSize="1" Enabled="true" Foreground="false" Comment="" LogTraceEvents="false" Schedule="">
       <Setting Target="Host" Name="TargetConfigName">CreditRiskProcess</Setting>
       <Setting Target="Adapter" Name="GroupID">iris</Setting>
       <Setting Target="Adapter" Name="Topic">CreditRiskInTopic</Setting>
       <Setting Target="Adapter" Name="Servers">kafka:9092</Setting>
    </Item>


    <Item Name="CreditRiskProcess" Category="" ClassName="dc.creditrisk.pyprod.CreditRiskProcess" PoolSize="1" Enabled="true" Foreground="false" Comment="" LogTraceEvents="false" Schedule="">
       <Setting Target="Host" Name="TargetConfigName">CreditRiskOperation</Setting>
    </Item>


    <Item Name="CreditRiskOperation" Category="" ClassName="EnsLib.Kafka.Operation" PoolSize="1" Enabled="true" Foreground="false" Comment="" LogTraceEvents="false" Schedule="">
       <Setting Target="Adapter" Name="ProducerClientID">iris</Setting>
       <Setting Target="Adapter" Name="Servers">kafka:9092</Setting>
    </Item>

</Production>
}

}
