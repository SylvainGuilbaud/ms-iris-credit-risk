"""Pure-Python port of the Credit Risk Kafka flow, built with intersystems_pyprod.

Python equivalents of:
  dc.creditrisk.CreditRiskPredictService  (Business Service, Kafka inbound)
  dc.creditrisk.CreditRiskProcess         (Business Process, runs PREDICT and publishes)

Load into IRIS with:   intersystems_pyprod /full/path/to/python/creditrisk_pyprod.py
Generated classes land in the package below (dc.creditrisk.pyprod), so they do
not collide with the ObjectScript originals.
"""
import json

import iris
from intersystems_pyprod import (
    BusinessProcess,
    BusinessService,
    IRISLog,
    IRISParameter,
    IRISProperty,
    OperationItem,
    ProcessItem,
    Production,
    ServiceItem,
    Status,
)

iris_package_name = "dc.creditrisk.pyprod"

TARGET_SETTING = (
    "Basic:selector?context={Ens.ContextSearch/ProductionItems"
    "?targets=1&productionName=@productionId}"
)

PREDICT_SQL = (
    "SELECT PREDICT(CreditRiskModel) AS RiskPrediction, "
    "       PROBABILITY(CreditRiskModel FOR 1) AS RiskProbability "
    "  FROM (SELECT ? As Age, ? As Sex, ? As Job, ? As Housing, ? As SavingAccounts, "
    "               ? As CheckingAccount, ? As CreditAmount, ? As Duration, ? As Purpose)"
)

FEATURES = (
    "Age", "Sex", "Job", "Housing", "SavingAccounts",
    "CheckingAccount", "CreditAmount", "Duration", "Purpose",
)


class CreditRiskPredictService(BusinessService):
    """Reads Kafka messages and forwards the payload to the target process."""

    ADAPTER = IRISParameter("EnsLib.Kafka.InboundAdapter")

    target_config_name: str = IRISProperty(
        default="CreditRiskProcess",
        description="Config name of the business process receiving the Kafka payload",
        settings=TARGET_SETTING,
    )

    def OnProcessInput(self, input):
        # input is an EnsLib.Kafka.Message
        request = iris.cls("Ens.StringRequest")._New()
        request.StringValue = input.value

        status = self.send_request_async(self.target_config_name, request)

        response = iris.cls("Ens.StringResponse")._New()
        response.StringValue = "OK"
        return status, response


class CreditRiskProcess(BusinessProcess):
    """Runs the IntegratedML prediction and publishes the result to Kafka."""

    target_config_name: str = IRISProperty(
        default="CreditRiskOperation",
        description="Config name of the business operation publishing to Kafka",
        settings=TARGET_SETTING,
    )

    def OnRequest(self, request):
        try:
            data = json.loads(request.StringValue)

            rs = iris.sql.exec(PREDICT_SQL, *[data.get(f) for f in FEATURES])
            row = next(iter(rs))
            prediction, probability = row[0], row[1]

            result = {f: data.get(f) for f in FEATURES}
            result["RiskPrediction"] = prediction
            result["RiskProbability"] = probability
            result["RiskAnalisysPrediction"] = (
                "Good credit" if str(prediction) == "1" else "Poor credit"
            )

            message = iris.cls("EnsLib.Kafka.Message")._New()
            message.topic = "CreditRiskOutTopic"
            message.value = json.dumps(result)
            message.key = "iris"

            status = self.send_request_async(
                self.target_config_name, message, response_required=0
            )
            if iris.cls("%SYSTEM.Status").IsError(status):
                return status

            response = iris.cls("Ens.StringResponse")._New()
            response.StringValue = "OK"
            return Status.OK(), response
        except Exception as ex:
            IRISLog.Error(f"CreditRiskProcess failed: {ex}")
            return Status.ERROR(str(ex))


class CreditRiskPyProduction(Production):
    """Python-defined production. Do not edit it from the UI (see pyprod docs)."""

    description = "Credit risk scoring over Kafka, implemented with intersystems_pyprod"

    services = [
        ServiceItem(
            "CreditRiskPredictService",
            f"{iris_package_name}.CreditRiskPredictService",
            host_settings={"TargetConfigName": "CreditRiskProcess"},
            adapter_settings={
                "GroupID": "iris",
                "Topic": "CreditRiskInTopic",
                "Servers": "kafka:9092",
            },
        )
    ]
    processes = [
        ProcessItem(
            "CreditRiskProcess",
            f"{iris_package_name}.CreditRiskProcess",
            host_settings={"TargetConfigName": "CreditRiskOperation"},
            pool_size=1,
        )
    ]
    operations = [
        OperationItem(
            "CreditRiskOperation",
            "EnsLib.Kafka.Operation",
            adapter_settings={"ProducerClientID": "iris", "Servers": "kafka:9092"},
        )
    ]
