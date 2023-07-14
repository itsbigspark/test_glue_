from src.cmph import CMPH
from src.psc import PSC
import boto3
from src.models.AuditLogsModel import AuditLogs

if __name__ == "__main__":
    cmph = CMPH()
    cmph.process()
    # psc = PSC()
    # psc.process()
    # audit = AuditLogs()
    # audit.save()
    # audit.delete_table()
    # audit.create_table()
    # a = audit.show_all()
    # print(a.current_rows)
