# Create packages and modules in the project and try to access them

from airflow.decorators import dag, task
import pendulum
from my_packages.packages_a.module_a import TestClass
from my_packages.packages_a.subpackage_a.subpackage_module_a import SubTestClass

@dag(schedule = None, start_date = pendulum.datetime(2024, 7, 1), catchup = False)

def test_module():
   
   @task
   def test_task():
       print(TestClass.my_time())

   @task
   def test_task2():
       print(SubTestClass.sub_my_time())

   test_task() >> test_task2()

dag = test_module()