from datetime import datetime
# from airflow.utils import timezone

from dateutil.relativedelta import relativedelta


def main():
    # Ref: https://airflow.apache.org/docs/stable/faq.html#what-s-the-deal-with-start-date
    # return datetime.combine(timezone.utcnow() - relativedelta(months=1), datetime.min.time()).replace(day=1)
    print(datetime.combine(datetime.now(),datetime.min.time()).replace(day=1) - relativedelta(seconds=1))
    print(datetime.combine(datetime.now() - relativedelta(months=1),datetime.min.time()).replace(day=1))
    print(datetime.combine(datetime.now(),datetime.min.time()).replace(day=1))
    print(datetime.combine(datetime.now(),datetime.min.time()))
    print(datetime.now())

if __name__ == "__main__":
    main()