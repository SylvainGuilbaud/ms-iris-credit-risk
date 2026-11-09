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

# Production Configuration draws a connector line between two items only for
# properties typed Ens.DataType.ConfigName. pyprod maps every datatype it does
# not know to %VarString, so register this one while the classes are generated.
# NOTE: pyprod reads IRISProperty/IRISParameter arguments from the source text
# (AST), so they must be literals, not variables.
try:
    from intersystems_pyprod import _parser

    _parser.DATATYPE_MAP.setdefault("Ens.DataType.ConfigName", "Ens.DataType.ConfigName")
except ImportError:  # not running under the intersystems_pyprod loader
    pass

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


def target_exists(target: str) -> bool:
    """True if `target` is a config item of the active production."""
    production = iris.cls("Ens.Director").GetActiveProductionName()
    rs = iris.sql.exec(
        "SELECT 1 FROM Ens_Config.Item WHERE Production = ? AND Name = ?",
        production,
        target,
    )
    return next(iter(rs), None) is not None


def alert_missing_target(host: str, target: str):
    """Raise an IRIS alert (Ens.Alert) and return an error status."""
    message = f"{host}: target '{target}' does not exist in the production"
    IRISLog.Alert(message)
    return Status.ERROR(message)


class CreditRiskPredictService(BusinessService):
    """Reads Kafka messages and forwards the payload to the target process."""

    ADAPTER = IRISParameter("EnsLib.Kafka.InboundAdapter")

    target_config_name: str = IRISProperty(
        # pyprod only quotes the default of %VarString properties, so quote it here
        default='"CreditRiskProcess"',
        datatype="Ens.DataType.ConfigName",
        description="Config name of the business process receiving the Kafka payload",
        settings="Basic:selector?context={Ens.ContextSearch/ProductionItems?targets=1&productionName=@productionId}",
    )

    def OnProcessInput(self, input):
        # input is an EnsLib.Kafka.Message
        if not target_exists(self.target_config_name):
            return alert_missing_target("CreditRiskPredictService", self.target_config_name)

        request = iris.cls("Ens.StringRequest")._New()
        request.StringValue = input.value

        status = self.send_request_async(self.target_config_name, request)

        response = iris.cls("Ens.StringResponse")._New()
        response.StringValue = "OK"
        return status, response


class CreditRiskProcess(BusinessProcess):
    """Runs the IntegratedML prediction and publishes the result to Kafka."""

    target_config_name: str = IRISProperty(
        default='"CreditRiskOperation"',
        datatype="Ens.DataType.ConfigName",
        description="Config name of the business operation publishing to Kafka",
        settings="Basic:selector?context={Ens.ContextSearch/ProductionItems?targets=1&productionName=@productionId}",
    )

    def OnRequest(self, request):
        try:
            if not target_exists(self.target_config_name):
                return alert_missing_target("CreditRiskProcess", self.target_config_name)

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
