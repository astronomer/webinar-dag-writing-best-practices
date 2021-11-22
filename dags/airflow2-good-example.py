from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weekday import WeekDay


"""
This DAG is intended to be a demonstration of a number of core Airflow concepts related to pipeline authoring
including TaskFlow API, branching, Edge Labels, Jinja templating, dynamic task generation, Task Groups, and
Trigger Rules.
"""

# Reference data for determining the activity to perform per day of week.
DAY_ACTIVITY_MAPPING = {
    "monday": {"is_weekday": True, "activity": "guitar lessons"},
    "tuesday": {"is_weekday": True, "activity": "studying"},
    "wednesday": {"is_weekday": True, "activity": "soccer practice"},
    "thursday": {"is_weekday": True, "activity": "contributing to Airflow"},
    "friday": {"is_weekday": True, "activity": "family dinner"},
    "saturday": {"is_weekday": False, "activity": "going to the beach"},
    "sunday": {"is_weekday": False, "activity": "sleeping in"},
}


@task(multiple_outputs=True)
def _going_to_the_beach() -> Dict:
    return {
        "subject": "Beach day!",
        "body": "It's Saturday and I'm heading to the beach.<br><br>Come join me!<br>",
    }


def _get_activity(day_name) -> str:
    activity_id = DAY_ACTIVITY_MAPPING[day_name]["activity"].replace(" ", "_")

    if DAY_ACTIVITY_MAPPING[day_name]["is_weekday"]:
        return f"weekday_activities.{activity_id}"

    return f"weekend_activities.{activity_id}"


# When using the DAG decorator, the ``dag`` argument doesn't need to be specified for each task.
# The ``dag_id`` value defaults to the name of the function it is decorating.
@dag(
    start_date=datetime(2021, 6, 11),  # Best practice is to use a static start_date.
    max_active_runs=1,
    schedule_interval="@daily",
    # Default settings applied to all tasks within the DAG; can be overwritten at the task level.
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    default_view="graph",
    catchup=False,
    tags=["example"],
    doc_md=__doc__,
)
def airflow2_good_example():
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    check_day_of_week = BranchDayOfWeekOperator(
        task_id="check_day_of_week",
        week_day={WeekDay.SATURDAY, WeekDay.SUNDAY},
        follow_task_ids_if_true="weekend",
        follow_task_ids_if_false="weekday",
        use_task_execution_day=True,
    )

    weekend = DummyOperator(task_id="weekend")
    weekday = DummyOperator(task_id="weekday")

    # Templated value for determining the name of the day of week based on the start date of the DagRun.
    day_name = "{{ dag_run.start_date.strftime('%A').lower() }}"

    # Begin weekday tasks.
    with TaskGroup("weekday_activities") as weekday_activities:
        which_weekday_activity_day = BranchPythonOperator(
            task_id="which_weekday_activity_day",
            python_callable=_get_activity,
            op_args=[day_name],
        )

        for day, day_info in DAY_ACTIVITY_MAPPING.items():
            if day_info["is_weekday"]:
                day_of_week = Label(label=day)
                activity = day_info["activity"]

                do_activity = BashOperator(
                    task_id=activity.replace(" ", "_"),
                    bash_command=f"echo It's {day.capitalize()} and I'm busy with {activity}.",
                )

                # Declaring task dependencies within the `TaskGroup` via the classic bitshift operator.
                which_weekday_activity_day >> day_of_week >> do_activity

    # Begin weekend tasks.
    with TaskGroup("weekend_activities") as weekend_activities:
        which_weekend_activity_day = BranchPythonOperator(
            task_id="which_weekend_activity_day",
            python_callable=_get_activity,
            op_args=[day_name],
        )

        saturday = Label(label="saturday")
        sunday = Label(label="sunday")

        sleeping_in = BashOperator(task_id="sleeping_in", bash_command="sleep $[ ( $RANDOM % 30 )  + 1 ]s")

        going_to_the_beach = _going_to_the_beach()

        # Because the ``going_to_the_beach()`` function has ``multiple_outputs`` enabled, each dict key is
        # accessible as their own `XCom` key.
        inviting_friends = EmailOperator(
            task_id="inviting_friends",
            to="friends@community.com",
            subject=going_to_the_beach["subject"],
            html_content=going_to_the_beach["body"],
        )

        # Using ``chain()`` here for list-to-list dependencies which are not supported by the bitshift
        # operator and to simplify the notation for the desired dependency structure.
        chain(which_weekend_activity_day, [saturday, sunday], [going_to_the_beach, sleeping_in])

    # High-level dependencies.
    chain(begin, check_day_of_week, [weekday, weekend], [weekday_activities, weekend_activities], end)

dag = airflow2_good_example()