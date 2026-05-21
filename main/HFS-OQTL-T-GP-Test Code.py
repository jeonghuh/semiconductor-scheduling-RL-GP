import random
import copy
import time
import math
import statistics
import ast
from random import randint

# Jobs = [5, 7, 10, 20, 40, 60, 80, 100]
# Stages = [5, 7, 10]
# Machines = [3,5]
# TF = [0.2, 0.4, 0.6]
# RDD = [0.2, 0.5, 0.8]
# Rework_ST = [1, 2]
# Queue_T = [1, 2]

Jobs = [80,100,200]
Stages = [7,10]
Machines = [2,4]
TF = [0.6]
RDD = [0.8]
Rework_ST = [1]
Queue_T = [1]
Num_instance = 10 #데이터파일 돌릴 횟수(갯수)


# GP_Rules 파일을 읽어와서 리스트로 파싱
with open('100_GP_Rules.txt', 'r', encoding='utf-8') as f:
    content = f.read().strip()
    if content.startswith("GP_Rules ="):
        content = content.replace("GP_Rules =", "", 1).strip()
    GP_Rules = ast.literal_eval(content)

Rule_Voting = [0 for i in range(len(GP_Rules))]

class Job_T:
    def __init__(self, Index, Processing_Time, Queue_Time_Limits, DD,AT):
        self.JobIndex = Index
        self.PrRoute = 0
        
        self.PT = Processing_Time
        self.QTL = Queue_Time_Limits
        self.Due_Date = DD
        self.Arrival_Time = AT

        self.Waiting_Time = 0
        self.QTL_Waiting_Time = [0, 0] # QTL[0], QTL[1] 개별 대기시간 
        self.isReworked = 0
        self.isTerminated = 0
        self.Remaining_Time = 0 # Sum of processing times of successor operations including itself.
        self.Positive_Slack_Time = 0
        self.Negative_Slack_Time = 0
        self.Current_Flow_Time = 0
        self.Remaining_Ops = 0 # Number of successor operations including itself.
        self.Min_QTL = 0
        self.Avg_QTL = 0
        self.Next_PT = 0
        self.Min_PT = 0
        self.Avg_PT = 0
        self.Median_PT = 0
        self.Critical_Ratio = 0
        self.isReentered = 0

        self.Start_Time = []
        self.Completion_Time = []

class Machine_T:
    def __init__(self, Cur_J_ST, Cur_J_CT, Cur_J_State, Cur_J):
        self.Current_Job_Start_Time = Cur_J_ST
        self.Current_Job_Completion_Time = Cur_J_CT
        self.Current_State = Cur_J_State #0 = (idle), 1 = (working)
        self.Current_Job = Cur_J

class Stage_T:
    def __init__(self, M, Q):
        self.Macs = M
        self.Queue = Q

class RS_Machine_T:
    def __init__(self, Cur_J_ST, Cur_J_CT, Cur_J_State, Cur_J, list1):
        self.Current_R_Job_Completion_Time = Cur_J_CT
        self.Current_R_State = Cur_J_State #0 = (idle), 1 = (working)
        self.Current_R_Job = Cur_J
        self.Queue = list1
        
def Dispatching_EDD(Queue):
    Queue = sorted(Queue, key=lambda x: x.Due_Date)
    return Queue

def Dispatching_MDD(Queue, index, time):
    for i in range(len(Queue)):
        for j in range(index, Stage_Number):
            Queue[i].Remaining_Time += Queue[i].PT[Queue[i].PrRoute][j]
    
    Queue = sorted(Queue, key=lambda x: max((x.Remaining_Time+time), x.Due_Date))
    return Queue

def Dispatching_MDDa(Queue, index, time):
    for i in range(len(Queue)):
        for j in range(index, Stage_Number):
            Queue[i].Remaining_Time += Queue[i].PT[Queue[i].PrRoute][j]

    Queue = sorted(Queue, key=lambda x: max((x.Due_Date - (x.Remaining_Time - x.PT[0][index]), time + x.PT[0][index])))
    return Queue

def Dispatching_MST(Queue, index, time):
    for i in range(len(Queue)):
        for j in range(index, Stage_Number):
            Queue[i].Remaining_Time += Queue[i].PT[Queue[i].PrRoute][j]

    Queue = sorted(Queue, key=lambda x: (x.Due_Date - time - x.PT[0][index]))
    return Queue

def Dispatching_FCFS(Queue):

    return Queue

def Event_Excuter(Stg, R_Mac, Dummy_Queue, Simu_Time, GP_R):
    Ev = 0
    Picked_Job = None
    Reworked_Job = None

    for i in range(len(Stg)):
        for j in range(len(Stg[i].Macs)):
            if Stg[i].Macs[j].Current_State == 0 and len(Stg[i].Queue) > 0:
                Rule_Eval = []
                for k in range(len(Stg[i].Queue)):
                    Stg[i].Queue[k].Remaining_Time = 0
                    for l in range(i, len(Stg)):
                        Stg[i].Queue[k].Remaining_Time += Stg[i].Queue[k].PT[Stg[i].Queue[k].PrRoute][l]
                    Stg[i].Queue[k].Current_Flow_Time = max(0, Simu_Time - Stg[i].Queue[k].Arrival_Time)
                    Stg[i].Queue[k].Positive_Slack_Time = max(0, Stg[i].Queue[k].Due_Date - Simu_Time)
                    Stg[i].Queue[k].Negative_Slack_Time = min(0, Stg[i].Queue[k].Due_Date - Simu_Time)
                    Stg[i].Queue[k].Remaining_Ops = len(Stg) - i
                    Stg[i].Queue[k].Min_QTL = min(Stg[i].Queue[k].QTL)
                    Stg[i].Queue[k].Avg_QTL = statistics.mean(Stg[i].Queue[k].QTL)
                    if i != len(Stg) - 1:
                        Stg[i].Queue[k].Next_PT = Stg[i].Queue[k].PT[0][i+1]
                    Stg[i].Queue[k].Min_PT = min(Stg[i].Queue[k].PT[0])
                    Stg[i].Queue[k].Avg_PT = statistics.mean(Stg[i].Queue[k].PT[0])
                    Stg[i].Queue[k].Median_PT = statistics.median(Stg[i].Queue[k].PT[0])
                    Stg[i].Queue[k].Critical_Ratio = (Stg[i].Queue[k].Due_Date - Stg[i].Queue[k].Current_Flow_Time) / Stg[i].Queue[k].PT[0][i]
                    Rule_Eval.append(eval(GP_R))
                
                Index = Rule_Eval.index(min(Rule_Eval))
                Picked_Job = Stg[i].Queue[Index]

                if Picked_Job != None:
                    if(Stg[i].Macs[j].Current_State == 0):
                        Stg[i].Macs[j].Current_Job = Picked_Job
                        Stg[i].Macs[j].Current_Job.isTerminated = 0
                        del[Stg[i].Queue[Index]]
                        Stg[i].Macs[j].Current_State = 1
                        Stg[i].Macs[j].Current_Job.Start_Time.append(Simu_Time)
                        Stg[i].Macs[j].Current_Job_Start_Time = Simu_Time
                        Stg[i].Macs[j].Current_Job_Completion_Time = Simu_Time + Stg[i].Macs[j].Current_Job.PT[0][i]
                        Ev = 1 
            
            if Stg[i].Macs[j].Current_Job_Completion_Time == Simu_Time:

                if i==4 and Stg[i].Macs[j].Current_Job.isReentered == 0: # 5번째 스테이지(인덱스 4) 완료 후 재진입
                    Stg[i].Macs[j].Current_Job.isReentered = 1 # re-enter 진입 상태로 업데이트
                    Stg[i].Macs[j].Current_Job.Completion_Time.append(Simu_Time)
                    Stg[i].Macs[j].Current_Job.QTL_Waiting_Time = [0, 0] # 재진입 시 QTL 개별 대기시간 초기화
                    Stg[0].Queue.append(Stg[i].Macs[j].Current_Job) # 1번 스테이지(인덱스 0) 큐로 다시 보내버림
                    # 머신 초기화
                    Stg[i].Macs[j].Current_Job.isTerminated = 1
                    Stg[i].Macs[j].Current_Job = None
                    Stg[i].Macs[j].Current_State = 0
                    Stg[i].Macs[j].Current_Job_Start_Time = -1
                    Stg[i].Macs[j].Current_Job_Completion_Time = -1
                    Ev = 1

                elif i < Stage_Number - 1: #아직 스테이지 남았으면 
                    Stg[i].Macs[j].Current_Job.Completion_Time.append(Simu_Time)
                    Stg[i+1].Queue.append(Stg[i].Macs[j].Current_Job)
                    Stg[i].Macs[j].Current_Job.isTerminated = 1
                    Stg[i].Macs[j].Current_Job = None
                    Stg[i].Macs[j].Current_State = 0
                    Stg[i].Macs[j].Current_Job_Start_Time = -1
                    Stg[i].Macs[j].Current_Job_Completion_Time = -1
                    Ev = 1
                
                elif (i == Stage_Number - 1) and (Stg[i].Macs[j].Current_Job.isReentered == 1): #마지막스테이지라면 
                    Stg[i].Macs[j].Current_Job.Completion_Time.append(Simu_Time)
                    Dummy_Queue.append(Stg[i].Macs[j].Current_Job)
                    Stg[i].Macs[j].Current_Job.isTerminated = 1
                    Stg[i].Macs[j].Current_Job = None
                    Stg[i].Macs[j].Current_State = 0
                    Stg[i].Macs[j].Current_Job_Start_Time = -1
                    Stg[i].Macs[j].Current_Job_Completion_Time = -1
                    Ev = 1  
                    a = 0

            # 각 stage의 queue에 위치한 job마다 queue time limit check (2-4 3-5 스테이지만 큐타임리밋 존재한다고 가정하고 있기 때문에 그 스테이지에서만 확인)
        if i == 2: #0부터 시작이니까 3번째 스테이지의 QTL[0] 확인 
            if len(Stg[i].Queue) > 0: #큐에 잡이 하나라도 있으면 
                A = Stg[i].Queue[:]
                for k in range(len(Stg[i].Queue)):
                    if(Stg[i].Queue[k].isReworked == 0 and (Stg[i].Queue[k].Waiting_Time > Stg[i].Queue[k].QTL[0])):
                        Stg[i].Queue[k].PrRoute = 1
                        Stg[i].Queue[k].Violated_QTL = 1
                        Stg[i].Queue[k].isReworked = 1
                        R_Mac.Queue.append(Stg[i].Queue[k])

                        Index = A.index(Stg[i].Queue[k])
                        del[A[Index]]
                        Ev = 1 #잡이 대기시간 제약을 초과한것도 event 발생1
                Stg[i].Queue = A
        
        if i == 3: #4번째 스테이지. QTL[0] 확인 
            if len(Stg[i].Queue) > 0:
                A = Stg[i].Queue[:]
                for k in range(len(Stg[i].Queue)):
                    if(Stg[i].Queue[k].isReworked == 0 and (Stg[i].Queue[k].QTL_Waiting_Time[0] > Stg[i].Queue[k].QTL[0])):
                        if Stg[i].Queue[k].isReentered==1:
                            Stg[i].Queue[k].PrRoute = 2 #1 2 3 4 5 1 2 3 100 102 103 4 5 6 7~
                        elif Stg[i].Queue[k].isReentered==0:
                            Stg[i].Queue[k].PrRoute = 6 #1 2 3 100 102 103 4 5 1 2 3 4 5 6 7~
                        Stg[i].Queue[k].Violated_QTL = 1
                        Stg[i].Queue[k].isReworked = 1
                        R_Mac.Queue.append(Stg[i].Queue[k])

                        Index = A.index(Stg[i].Queue[k])
                        del[A[Index]]
                        Ev = 1
                Stg[i].Queue = A

        if i == 3: #4번째 스테이지 QTL[1] 확인
            if len(Stg[i].Queue) > 0:
                A = Stg[i].Queue[:]
                for k in range(len(Stg[i].Queue)):
                    if(Stg[i].Queue[k].isReworked == 0 and (Stg[i].Queue[k].QTL_Waiting_Time[1] > Stg[i].Queue[k].QTL[1])):
                        if Stg[i].Queue[k].isReentered==1:
                            Stg[i].Queue[k].PrRoute = 3 #1 2 3 4 5 1 2 3 100 103 4 5 6 7
                        elif Stg[i].Queue[k].isReentered==0:
                            Stg[i].Queue[k].PrRoute = 7 #1 2 3 100 103 4 5 1 2 3 4 5 6 7
                        Stg[i].Queue[k].Violated_QTL = 2
                        Stg[i].Queue[k].isReworked = 1
                        R_Mac.Queue.append(Stg[i].Queue[k])

                        Index = A.index(Stg[i].Queue[k])
                        del[A[Index]]
                        Ev = 1
                Stg[i].Queue = A                        

        if i == 4: #5번째 스테이지 QTL[1] 확인 
            if len(Stg[i].Queue) > 0:
                A = Stg[i].Queue[:]
                for k in range(len(Stg[i].Queue)):
                    if(Stg[i].Queue[k].isReworked == 0 and (Stg[i].Queue[k].QTL_Waiting_Time[1] > Stg[i].Queue[k].QTL[1])):
                        if Stg[i].Queue[k].isReentered==1:
                            Stg[i].Queue[k].PrRoute = 4 #1 2 3 4 5 1 2 3 4 100 103 104 5 6 7
                        elif Stg[i].Queue[k].isReentered==0:
                            Stg[i].Queue[k].PrRoute = 8 #1 2 3 4 100 103 104 5 1 2 3 4 5 6 7
                        Stg[i].Queue[k].Violated_QTL = 2
                        Stg[i].Queue[k].isReworked = 1
                        R_Mac.Queue.append(Stg[i].Queue[k])

                        Index = A.index(Stg[i].Queue[k])
                        del[A[Index]]
                        Ev = 1
                Stg[i].Queue = A 

    if R_Mac.Current_R_State == 0 and len(R_Mac.Queue) > 0:
        Rule_Eval = []
        for k in range(len(R_Mac.Queue)):
            job = R_Mac.Queue[k]
            
            job.Remaining_Time = job.PT[1][2]
            v_stg = job.Violated_QTL
            for l in range(v_stg, len(Stg)):
                job.Remaining_Time += job.PT[0][l]
            job.Positive_Slack_Time = max(0, job.Due_Date - Simu_Time)
            job.Negative_Slack_Time = min(0, job.Due_Date - Simu_Time)
            job.Current_Flow_Time = max(0, Simu_Time - job.Arrival_Time)
            job.Remaining_Ops = len(Stg) - v_stg + 1
            job.Min_QTL = min(job.QTL)
            job.Avg_QTL = statistics.mean(job.QTL)
            job.Next_PT = job.PT[0][v_stg]
            actual_pt_list = job.PT[0] + [job.PT[1][2]] 
            job.Min_PT = min(actual_pt_list)
            job.Avg_PT = statistics.mean(actual_pt_list)
            job.Median_PT = statistics.median(actual_pt_list)
            job.Critical_Ratio = (job.Due_Date - job.Current_Flow_Time) / job.PT[1][2]
            rule_str = GP_R.replace('Stg[i]', 'R_Mac').replace('[0][i]', '[1][2]')
            Rule_Eval.append(eval(rule_str))
            
        Index = Rule_Eval.index(min(Rule_Eval))
        Reworked_Job = R_Mac.Queue[Index]

        if Reworked_Job != None:
            R_Mac.Current_R_Job = Reworked_Job
            R_Mac.Current_R_Job.Waiting_Time = 0
            R_Mac.Current_R_Job.QTL_Waiting_Time = [0, 0] # 재작업 시 QTL 대기시간 초기화
            R_Mac.Current_R_Job.isTerminated = 0
            del[R_Mac.Queue[Index]]

            R_Mac.Current_R_State = 1
            R_Mac.Current_R_Job.Start_Time.append(Simu_Time)
            R_Mac.Current_R_Job_Start_Time = Simu_Time
            R_Mac.Current_R_Job_Completion_Time = Simu_Time + R_Mac.Current_R_Job.PT[5][2]
            
            Ev = 1

    if R_Mac.Current_R_Job != None and R_Mac.Current_R_Job_Completion_Time == Simu_Time:
        R_Mac.Current_R_Job.Completion_Time.append(Simu_Time)
        R_Mac.Current_R_Job.isTerminated = 1
        R_Mac.Current_R_Job.isReworked = 1

        Stg[R_Mac.Current_R_Job.Violated_QTL].Queue.append(R_Mac.Current_R_Job)

        R_Mac.Current_R_Job = None
        R_Mac.Current_R_State = 0
        R_Mac.Current_R_Job_Start_Time = -1
        R_Mac.Current_R_Job_Completion_Time = -1

        Ev = 1

    return Ev

def Simulator(Stg, R_Mac, Jobs_List, GP_R):
    Simu_Time = 0
    Dummy_Queue = []
    TT = 0 # Total Tardiness
    TFT = 0 # Total Flow Time
    JN = len(Jobs_List)

    Sorted_Jobs = sorted(Jobs_List, key=lambda x: (x.Arrival_Time, x.JobIndex))

    for i in range(len(Stg)):
        Stg[i].Queue = []
        for j in range(len(Stg[i].Macs)):
            Stg[i].Macs[j].Current_Job_Start_Time = -1
            Stg[i].Macs[j].Current_Job_Completion_Time = -1
            Stg[i].Macs[j].Current_State = 0
            Stg[i].Macs[j].Current_Job = None
    
    R_Mac.Queue = []
    R_Mac.Current_Job_Start_Time = -1
    R_Mac.Current_Job_Completion_Time = -1
    R_Mac.Current_R_State = 0
    R_Mac.Current_R_Job = None

    while True:
        while Sorted_Jobs and Sorted_Jobs[0].Arrival_Time <= Simu_Time:
            Stg[0].Queue.append(Sorted_Jobs.pop(0))
            
        Event = 1

        while Event != 0:
            # Return 되는 Event가 1이면 while문 계속 반복해서 event check!
            Event = Event_Excuter(Stg, R_Mac, Dummy_Queue, Simu_Time, GP_R)

        # Stage 2-4 and Stage 3-5 사이에서 queue time limits check
        for i in range(2, 5):
            if len(Stg[i].Queue) > 0:
                for k in range(0, len(Stg[i].Queue)):
                    if Stg[i].Queue[k].isReworked == 0:
                        Stg[i].Queue[k].Waiting_Time += 1
                        if i in [2, 3]: # Stage 3, 4 큐: QTL[0] (Stage 2-4 구간) 대기시간
                            Stg[i].Queue[k].QTL_Waiting_Time[0] += 1
                        if i in [3, 4]: # Stage 4, 5 큐: QTL[1] (Stage 3-5 구간) 대기시간
                            Stg[i].Queue[k].QTL_Waiting_Time[1] += 1
        
        Simu_Time += 1
        # Calculate the total tardiness
        if len(Dummy_Queue) == JN:
            for i in range(0, len(Dummy_Queue)):
                TT += max(0, (Dummy_Queue[i].Completion_Time[len(Dummy_Queue[i].Completion_Time) - 1] - Dummy_Queue[i].Due_Date))
                # TFT += Dummy_Queue[i].Completion_Time[len(Dummy_Queue[i].Completion_Time) - 1]
                TFT += (Dummy_Queue[i].Completion_Time[len(Dummy_Queue[i].Completion_Time) - 1] - Dummy_Queue[i].Arrival_Time)
            break

    return TT

# Test Phase
for Job in Jobs:
    for Stage in Stages:
        for Machine in Machines:
            for RS in Rework_ST:
                for Q in Queue_T:
                    for T in TF:
                        for R in RDD:
                            for Instance in range(Num_instance):
                                # File = open("Test Data/Data_" + str(RS) + "_" + str(Q) + "/Data_" + str(T) + "_" + str(R) + "/Data_" + 
                                #         str(Job) + "_" + str(Stage) + "_" + str(Machine) + "_" + str(Instance) + ".txt")
                                File = open("Test_Data/" + str(Job) + "_" + str(Stage) + "_" + str(Machine) + "_" + str(Instance) + ".txt")
                                
                                # File_w = open("F:/1-Research/1-Journals/1-Working/0-S-HFSS-OQTL (Chapter 2)/1-Code/4-HFS-OQTL-MFSGP/1-Test Results/Result_" + str(RS) + "_" + str(Q) + "/Result_" + str(T) + "_" + str(R) + "/Result_" + 
                                #         str(Job) + "_" + str(Stage) + "_" + str(Machine) + "_" + str(Instance) + ".txt", 'w')
                                File_w = open("Test_Results/" + str(Job) + "_" + str(Stage) + "_" + str(Machine) + "_" + str(Instance) + ".txt", 'w')

                                Data = File.readline()
                                Tmp = []

                                while Data != '':
                                    Tmp.append(Data)
                                    Data = File.readline()
                                
                                Tmp = list(map(lambda s:s.rstrip(), Tmp))

                                Job_Number = int(Tmp[0])
                                Stage_Number = int(Tmp[1])
                                Machine_Number = int(Tmp[2])
                                QTLs_Number = int(Tmp[3])
                                Process_Route_Number = 9
                                del Tmp[0:5]

                                Arrival_Time = []
                                for i in range(0, Job_Number):
                                    Arrival_Time.append(int(Tmp[i]))
                                del Tmp[0:Job_Number + 1]

                                Queue_Time_Limits = []
                                for i in range(0, Job_Number):
                                    Queue_Time_Limits.append(Tmp[i])
                                del Tmp[0:Job_Number + 1]
                                
                                for i in range(0, len(Queue_Time_Limits)):
                                    Queue_Time_Limits[i] = Queue_Time_Limits[i].split(' ')

                                Queue_Time_Limits = [list(map(int, i)) for i in Queue_Time_Limits]

                                Due_Date = []
                                for i in range(0, Job_Number):
                                    Due_Date.append(int(Tmp[i]))
                                del Tmp[0:Job_Number + 1]

                                # Process routes 수만큼 data 불러오기
                                Process_Routes = []
                                
                                for i in range(0, Process_Route_Number):
                                    Process_Routes.append(Tmp[i])
                                del Tmp[0:Process_Route_Number+1]

                                for i in range(0, len(Process_Routes)):
                                    Process_Routes[i] = Process_Routes[i].split(' ')

                                Process_Routes = [list(map(int, i)) for i in Process_Routes]


                                # 각 Job마다 Process route 별 processing time
                                Processing_Time = []

                                for i in range(0, Job_Number):
                                    TmpList = []
                                    for j in range(0, Process_Route_Number):
                                        TmpList.append(Tmp[j])
                                    Processing_Time.append(TmpList)
                                    del Tmp[0: Process_Route_Number + 1]
                                
                                for i in range(0, Job_Number):
                                    for j in range(0, Process_Route_Number):
                                        Processing_Time[i][j] = Processing_Time[i][j].split(' ')
                                        Processing_Time[i][j] = list(map(int, Processing_Time[i][j]))
                                
                                Stages_T = []                        
                                for i in range(Stage_Number):
                                    Stage_Q = []
                                    Machines_T = []
                                    Cur_J_ST = 0
                                    Cur_J_CT = 0
                                    Cur_J_State = 0
                                    Cur_J = None

                                    for j in range(Machine_Number):
                                        Machines_T.append(Machine_T(Cur_J_ST, Cur_J_CT, Cur_J_State, Cur_J))
                                    Stages_T.append(Stage_T(Machines_T, Stage_Q))
                                                    
                                RS_Mac_Q = []
                                RS_Mac = RS_Machine_T(Cur_J_ST, Cur_J_CT, Cur_J_State, Cur_J, RS_Mac_Q)
                                Dummy_Queue = []
                                
                                Start_Time = time.process_time()

                                Total_Tardiness = []
                                eval_start_time = time.time()

                                for idx, Rule in enumerate(GP_Rules):
                                    elapsed = time.time() - eval_start_time
                                    print(f"\r▶ 테스트 실행 중... {elapsed:.1f}초 경과 [Job: {Job}, Stage: {Stage}, M: {Machine}, Inst: {Instance}] (규칙 평가: {idx + 1}/{len(GP_Rules)})", end="", flush=True)
                                    
                                    Jobs_T = []
                                    for i in range(0, Job_Number):
                                        Jobs_T.append(Job_T(i, Processing_Time[i], Queue_Time_Limits[i], Due_Date[i],Arrival_Time[i]))
                                    
                                    Total_Tardiness.append(Simulator(Stages_T, RS_Mac, Jobs_T, Rule))
                                print() # 다음 인스턴스로 넘어갈 때 출력이 겹치지 않게 줄바꿈 처리
                                
                                # # Total_Tardiness와 GP_Rules를 함께 정렬
                                # sorted_pairs = sorted(zip(Total_Tardiness, GP_Rules))

                                Sorted_TT = sorted(Total_Tardiness)
                                # # 정렬된 결과를 다시 리스트에 분리
                                # Total_Tardiness, GP_Rules = zip(*sorted_pairs)

                                # # 리스트를 다시 원래 형태로 변환 (튜플을 리스트로 변환)
                                # Total_Tardiness = list(Total_Tardiness)
                                # GP_Rules = list(GP_Rules)

                                Min_Index = sorted(range(len(Total_Tardiness)), key=lambda x: Total_Tardiness[x])[:4]
                                
                                # Increment corresponding GP_Voting values by 1
                                for idx in Min_Index:
                                    Rule_Voting[idx] += 1
                                
                                Initial_End_Time = time.process_time()
                                Initial_Time = Initial_End_Time - Start_Time

                                File_w.write("CPU Time = ")
                                File_w.write(str(Initial_Time))
                                File_w.write("\n")
                                File_w.write("\n")

                                File_w.write("Total_Tardiness = ")
                                File_w.write(str(Sorted_TT))
                                File_w.write("\n")
                                File_w.write("\n")

                                File_w.write("GP_Rule Voting:\n")  # GP_Rules의 헤더 출력
                                for rule in range(len(GP_Rules)):
                                    File_w.write(str(Rule_Voting[rule]) + "\n")
                                
                                File_w.close()

# 모든 테스트가 끝난 후 전체 누적 투표 결과 상위 5개 GP Rule을 추출하여 새 파일에 저장
Top5_Index = sorted(range(len(Rule_Voting)), key=lambda x: Rule_Voting[x], reverse=True)[:5]

with open("Test_Results/Top5_GP_Rules.txt", "w", encoding="utf-8") as f_top:
    f_top.write("=== Top 5 GP Rules by Total Votes ===\n\n")
    for rank, idx in enumerate(Top5_Index, 1):
        f_top.write(f"Rank {rank} (Votes: {Rule_Voting[idx]}): \n{GP_Rules[idx]}\n\n")

print("\n▶ 모든 테스트가 완료되었습니다. 상위 5개의 룰이 'Top5_GP_Rules.txt'에 저장되었습니다.")
                            