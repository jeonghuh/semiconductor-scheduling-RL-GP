import ast
import glob
import os
import random
import statistics
import time
import zipfile
from collections import deque
from datetime import datetime
from html import escape

import numpy as np
import matplotlib.pyplot as plt

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
except ImportError:
    torch = None
    nn = None
    optim = None




Jobs = [10]
Stages = [7]
Machines = [3]
TF = [0.6]
RDD = [0.8]
Rework_ST = [1]
Queue_T = [1]
Num_instance = 5


# ======================== 1. 공정 설계 ========================
class Job_T:
    def __init__(self, Index, Processing_Time, Queue_Time_Limits, DD, AT):
        self.JobIndex = Index
        self.PrRoute = 0

        self.PT = Processing_Time
        self.QTL = Queue_Time_Limits
        self.Due_Date = DD
        self.Arrival_Time = AT

        self.Waiting_Time = 0
        self.QTL_Waiting_Time = [0, 0]
        self.isReworked = 0
        self.isTerminated = 0
        self.Remaining_Time = 0
        self.Positive_Slack_Time = 0
        self.Negative_Slack_Time = 0
        self.Current_Flow_Time = 0
        self.Remaining_Ops = 0
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
        self.Current_State = Cur_J_State
        self.Current_Job = Cur_J
        self.Busy_Time = 0


class Stage_T:
    def __init__(self, M, Q):
        self.Macs = M
        self.Queue = Q


class RS_Machine_T:
    def __init__(self, Cur_J_ST, Cur_J_CT, Cur_J_State, Cur_J, list1):
        self.Current_R_Job_Start_Time = Cur_J_ST
        self.Current_R_Job_Completion_Time = Cur_J_CT
        self.Current_R_State = Cur_J_State
        self.Current_R_Job = Cur_J
        self.Queue = list1
        self.Busy_Time = 0


# ======================== 2. DataLoader ========================
class DataLoader:
    def __init__(self, input_file):
        self.input_file = input_file
        self.Process_Route_Number = 9

        with open(self.input_file, "r") as file:
            tmp = [line.rstrip() for line in file]

        self.Job_Number = int(tmp[0])
        self.Stage_Number = int(tmp[1])
        self.Machine_Number = int(tmp[2])
        self.QTLs_Number = int(tmp[3])
        del tmp[0:5]

        self.Arrival_Time = [int(tmp[i]) for i in range(self.Job_Number)]
        del tmp[0:self.Job_Number + 1]

        self.Queue_Time_Limits = [
            list(map(int, tmp[i].split(" "))) for i in range(self.Job_Number)
        ]
        del tmp[0:self.Job_Number + 1]

        self.Due_Date = [int(tmp[i]) for i in range(self.Job_Number)]
        del tmp[0:self.Job_Number + 1]

        self.Process_Routes = [
            list(map(int, tmp[i].split(" "))) for i in range(self.Process_Route_Number)
        ]
        del tmp[0:self.Process_Route_Number + 1]

        self.Processing_Time = []
        for _ in range(self.Job_Number):
            tmp_list = []
            for j in range(self.Process_Route_Number):
                tmp_list.append(list(map(int, tmp[j].split(" "))))
            self.Processing_Time.append(tmp_list)
            del tmp[0:self.Process_Route_Number + 1]

        self.stages = []
        for _ in range(self.Stage_Number):
            machines_t = []
            stage_q = []
            for _ in range(self.Machine_Number):
                machines_t.append(Machine_T(0, 0, 0, None))
            self.stages.append(Stage_T(machines_t, stage_q))

        self.r_mac = RS_Machine_T(0, 0, 0, None, [])
        self.jobs = [
            Job_T(
                i,
                self.Processing_Time[i],
                self.Queue_Time_Limits[i],
                self.Due_Date[i],
                self.Arrival_Time[i],
            )
            for i in range(self.Job_Number)
        ]

    def get_simulation_objects(self):
        return self.stages, self.r_mac, self.jobs


# ======================== 3. Rules ========================
TOP_K_RULES = 5

def load_gp_rules(path="formatted_GP_Rules.txt", top_k=TOP_K_RULES):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if content.startswith("GP_Rules ="):
            content = content.replace("GP_Rules =", "", 1).strip()
        rules = ast.literal_eval(content)
    return rules[:top_k]


GP_Rules = load_gp_rules()

BASELINE_RESULT_CACHE = {}


# ======================== 4. Event Excuter ========================
def Event_Excuter(
    Stg,
    R_Mac,
    Dummy_Queue,
    Simu_Time,
    GP_Rules,
    agent=None,
    get_state_fn=None,
    reward_fn=None,
    done_fn=None,
    JN=1,
    store_transitions=True,
):
    Ev = 0
    stored_transitions = 0

    for i in range(len(Stg)):
        for j in range(len(Stg[i].Macs)):
            if Stg[i].Macs[j].Current_State == 0 and len(Stg[i].Queue) > 0:
                state = get_state_fn() if get_state_fn is not None else None
                if agent is None:
                    action = random.randrange(len(GP_Rules))
                else:
                    action = agent.select_action(state)
                GP_R = GP_Rules[action]

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
                        Stg[i].Queue[k].Next_PT = Stg[i].Queue[k].PT[0][i + 1]
                    else:
                        Stg[i].Queue[k].Next_PT = 0
                    Stg[i].Queue[k].Min_PT = min(Stg[i].Queue[k].PT[0])
                    Stg[i].Queue[k].Avg_PT = statistics.mean(Stg[i].Queue[k].PT[0])
                    Stg[i].Queue[k].Median_PT = statistics.median(Stg[i].Queue[k].PT[0])
                    current_pt = max(1, Stg[i].Queue[k].PT[0][i])
                    Stg[i].Queue[k].Critical_Ratio = (Stg[i].Queue[k].Due_Date - Stg[i].Queue[k].Current_Flow_Time) / current_pt
                    Rule_Eval.append(eval(GP_R))

                Index = Rule_Eval.index(min(Rule_Eval))
                Picked_Job = Stg[i].Queue[Index]

                Stg[i].Macs[j].Current_Job = Picked_Job
                Stg[i].Macs[j].Current_Job.isTerminated = 0
                del Stg[i].Queue[Index]
                Stg[i].Macs[j].Current_State = 1
                Stg[i].Macs[j].Current_Job.Start_Time.append(Simu_Time)
                Stg[i].Macs[j].Current_Job_Start_Time = Simu_Time
                Stg[i].Macs[j].Current_Job_Completion_Time = (Simu_Time + Stg[i].Macs[j].Current_Job.PT[0][i])
                Stg[i].Macs[j].Busy_Time += (Stg[i].Macs[j].Current_Job_Completion_Time - Stg[i].Macs[j].Current_Job_Start_Time)

                info = {
                    "decision_type": "stage",
                    "stage": i,
                    "machine": j,
                    "job": Picked_Job.JobIndex,
                    "rule_action": action,
                    "time": Simu_Time,
                }
                if agent is not None and store_transitions:
                    next_state = get_state_fn() if get_state_fn is not None else None
                    done = done_fn() if done_fn is not None else len(Dummy_Queue) == JN
                    reward = reward_fn(info, done) if reward_fn is not None else 0.0
                    agent.store(state, action, reward, next_state, done)
                    stored_transitions += 1
                Ev = 1

            if (
                Stg[i].Macs[j].Current_Job is not None
                and Stg[i].Macs[j].Current_Job_Completion_Time == Simu_Time
            ):

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

                elif i < len(Stg) - 1: 
                    Stg[i].Macs[j].Current_Job.Completion_Time.append(Simu_Time)
                    Stg[i+1].Queue.append(Stg[i].Macs[j].Current_Job)
                    Stg[i].Macs[j].Current_Job.isTerminated = 1
                    Stg[i].Macs[j].Current_Job = None
                    Stg[i].Macs[j].Current_State = 0
                    Stg[i].Macs[j].Current_Job_Start_Time = -1
                    Stg[i].Macs[j].Current_Job_Completion_Time = -1
                    Ev = 1
                
                elif (i == len(Stg) - 1) and (Stg[i].Macs[j].Current_Job.isReentered == 1):
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
        state = get_state_fn() if get_state_fn is not None else None
        if agent is None:
            action = random.randrange(len(GP_Rules))
        else:
            action = agent.select_action(state)
        GP_R = GP_Rules[action]
        rule_str = GP_R.replace("Stg[i]", "R_Mac").replace("[0][i]", "[1][2]")

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
            Rule_Eval.append(eval(rule_str))

        Index = Rule_Eval.index(min(Rule_Eval))
        Reworked_Job = R_Mac.Queue[Index]

        R_Mac.Current_R_Job = Reworked_Job
        R_Mac.Current_R_Job.Waiting_Time = 0
        R_Mac.Current_R_Job.QTL_Waiting_Time = [0, 0] # 재작업 시 QTL 대기시간 초기화
        R_Mac.Current_R_Job.isTerminated = 0
        del R_Mac.Queue[Index]

        R_Mac.Current_R_State = 1
        R_Mac.Current_R_Job.Start_Time.append(Simu_Time)
        R_Mac.Current_R_Job_Start_Time = Simu_Time
        R_Mac.Current_R_Job_Completion_Time = Simu_Time + R_Mac.Current_R_Job.PT[5][2]
        R_Mac.Busy_Time += R_Mac.Current_R_Job_Completion_Time - R_Mac.Current_R_Job_Start_Time

        info = {
            "decision_type": "rework",
            "stage": None,
            "machine": None,
            "job": Reworked_Job.JobIndex,
            "rule_action": action,
            "time": Simu_Time,
        }
        if agent is not None and store_transitions:
            next_state = get_state_fn() if get_state_fn is not None else None
            done = done_fn() if done_fn is not None else len(Dummy_Queue) == JN
            reward = reward_fn(info, done) if reward_fn is not None else 0.0
            agent.store(state, action, reward, next_state, done)
            stored_transitions += 1
        Ev = 1

    if R_Mac.Current_R_Job is not None and R_Mac.Current_R_Job_Completion_Time == Simu_Time:
        R_Mac.Current_R_Job.Completion_Time.append(Simu_Time)
        R_Mac.Current_R_Job.isTerminated = 1
        R_Mac.Current_R_Job.isReworked = 1

        Stg[R_Mac.Current_R_Job.Violated_QTL].Queue.append(R_Mac.Current_R_Job)

        R_Mac.Current_R_Job = None
        R_Mac.Current_R_State = 0
        R_Mac.Current_R_Job_Start_Time = -1
        R_Mac.Current_R_Job_Completion_Time = -1
        Ev = 1

    return Ev, stored_transitions


# ======================== 5. Q-Network ========================
if nn is not None:

    class QNetwork(nn.Module):
        def __init__(self, input_dim, output_dim):
            super(QNetwork, self).__init__()
            self.model = nn.Sequential(
                nn.Linear(input_dim, 128),
                nn.ReLU(),
                nn.Linear(128, 64),
                nn.ReLU(),
                nn.Linear(64, output_dim),
            )

        def forward(self, x):
            return self.model(x)

else:

    class QNetwork:
        def __init__(self, *args, **kwargs):
            raise ImportError("PyTorch is required to use QNetwork.")


# ======================== 6. Replay Buffer ========================
class ReplayBuffer:
    def __init__(self, capacity):
        self.buffer = deque(maxlen=capacity)

    def push(self, s, a, r, s_, done):
        self.buffer.append((s, a, r, s_, done))

    def add_reward_to_last(self, reward_delta, next_state=None, done=None):
        if not self.buffer:
            return False
        s, a, r, s_, d = self.buffer[-1]
        if next_state is not None:
            s_ = next_state
        if done is not None:
            d = done
        self.buffer[-1] = (s, a, r + reward_delta, s_, d)
        return True

    def sample(self, batch_size):
        if torch is None:
            raise ImportError("PyTorch is required to sample ReplayBuffer tensors.")
        samples = random.sample(self.buffer, batch_size)
        s, a, r, s_, d = zip(*samples)
        return (
            torch.tensor(s, dtype=torch.float),
            torch.tensor(a, dtype=torch.long),
            torch.tensor(r, dtype=torch.float),
            torch.tensor(s_, dtype=torch.float),
            torch.tensor(d, dtype=torch.float),
        )

    def __len__(self):
        return len(self.buffer)


# ======================== 7. Hybrid Epsilon Scheduler ========================
class HybridEpsilonScheduler:
    def __init__(self, init=1.0, mid=0.2, final=0.05, total_episodes=1000):
        self.init = init
        self.mid = mid
        self.final = final
        self.total = total_episodes
        self.episode = 0
        self.epsilon = init

    def update(self, episode_reward):
        self.episode += 1
        ratio = self.episode / self.total
        if ratio < 0.3:
            self.epsilon = self.init
        elif ratio < 0.7:
            p = (ratio - 0.3) / 0.4
            self.epsilon = self.init - (self.init - self.mid) * p
        else:
            p = (ratio - 0.7) / 0.3
            self.epsilon = self.mid - (self.mid - self.final) * p

        return max(self.epsilon, self.final)


# ======================== 8. DQN Agent ========================
class DQNAgent:
    def __init__(self, state_dim, action_dim):
        if torch is None:
            raise ImportError("PyTorch is required to use DQNAgent.")

        self.q_net = QNetwork(state_dim, action_dim)
        self.target_net = QNetwork(state_dim, action_dim)
        self.target_net.load_state_dict(self.q_net.state_dict())
        self.optimizer = optim.Adam(self.q_net.parameters(), lr=1e-3)

        self.memory = ReplayBuffer(capacity=20000)
        self.batch_size = 64
        self.gamma = 0.99

        self.epsilon = 1.0
        self.action_dim = action_dim
        self.steps = 0
        self.train_start = 1000
        self.train_freq = 4
        self.target_update_freq = 500
        self.last_loss = None

    def select_action(self, state):
        if random.random() < self.epsilon:
            return random.randint(0, self.action_dim - 1)
        state = torch.FloatTensor(state).unsqueeze(0)
        with torch.no_grad():
            return self.q_net(state).argmax().item()

    def store(self, s, a, r, s_, done):
        self.memory.push(s, a, r, s_, done)

    def add_reward_to_last_transition(self, reward_delta, next_state=None, done=None):
        return self.memory.add_reward_to_last(reward_delta, next_state, done)

    def update(self):
        self.steps += 1
        if len(self.memory) < max(self.train_start, self.batch_size):
            return
        if self.steps % self.train_freq != 0:
            return

        s, a, r, s_, d = self.memory.sample(self.batch_size)
        q = self.q_net(s).gather(1, a.unsqueeze(1)).squeeze(1)
        q_target = r + self.gamma * self.target_net(s_).max(1)[0] * (1 - d)

        loss = nn.MSELoss()(q, q_target.detach())
        self.optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(self.q_net.parameters(), 1.0)
        self.optimizer.step()

        self.last_loss = loss
        # Target network is updated from validation-best checkpoints in the
        # training loop.


# ======================== 9. Stepwise Simulator ========================
class StepwiseSimulator:
    def __init__(self, input_path, gp_rules=None):
        self.input_path = input_path
        self.gp_rules = gp_rules if gp_rules is not None else GP_Rules
        self.action_dim = len(self.gp_rules)
        self.reset()

    def reset(self):
        self.reader = DataLoader(self.input_path)
        self.Stg, self.R_Mac, self.Jobs_List = self.reader.get_simulation_objects()
        self.Simu_Time = 0
        self.Dummy_Queue = []
        self.Sorted_Jobs = sorted(self.Jobs_List, key=lambda x: (x.Arrival_Time, x.JobIndex))
        self.JN = len(self.Jobs_List)
        self.num_jobs = self.JN
        self.last_step_reward_sum = 0.0
        self.last_step_reward_count = 0
        return self.get_state()

    def is_done(self):
        return len(self.Dummy_Queue) == self.JN

    def step(self, agent=None, store_transitions=True, reward_fn=None):
        if self.is_done():
            return 0, True, 0

        while self.Sorted_Jobs and self.Sorted_Jobs[0].Arrival_Time <= self.Simu_Time:
            self.Stg[0].Queue.append(self.Sorted_Jobs.pop(0))

        event = 1
        event_occurred = 0
        stored_transitions = 0
        self.last_step_reward_sum = 0.0
        self.last_step_reward_count = 0
        while event != 0:
            event, decision_count = Event_Excuter(
                self.Stg,
                self.R_Mac,
                self.Dummy_Queue,
                self.Simu_Time,
                self.gp_rules,
                agent=agent,
                get_state_fn=self.get_state,
                reward_fn=reward_fn,
                done_fn=self.is_done,
                JN=self.JN,
                store_transitions=store_transitions,
            )
            if event:
                event_occurred = 1
            stored_transitions += decision_count

        self.advance_waiting_times()
        self.Simu_Time += 1
        return event_occurred, self.is_done(), stored_transitions

    def advance_waiting_times(self):
        for i in range(2, 5):
            for job in self.Stg[i].Queue:
                if job.isReworked == 0:
                    job.Waiting_Time += 1
                    if i in [2, 3]:
                        job.QTL_Waiting_Time[0] += 1
                    if i in [3, 4]:
                        job.QTL_Waiting_Time[1] += 1

    def get_state(self):
        state = []
        stage_count = len(self.Stg)
        machines_per_stage = max(1, len(self.Stg[0].Macs) if self.Stg else 1)

        def active_entries():
            entries = []
            seen = set()

            def add(job, stage_idx=None, in_rework=False):
                if job is None or id(job) in seen:
                    return
                seen.add(id(job))
                entries.append((job, stage_idx, in_rework))

            for stage_idx, stage in enumerate(self.Stg):
                for job in stage.Queue:
                    add(job, stage_idx, False)
                for mac in stage.Macs:
                    add(mac.Current_Job, stage_idx, False)

            for job in self.R_Mac.Queue:
                add(job, None, True)
            add(self.R_Mac.Current_R_Job, None, True)
            return entries

        def remaining_processing_time(job, stage_idx=None, in_rework=False):
            if in_rework:
                violated_stage = getattr(job, "Violated_QTL", 0)
                rework_time = job.PT[1][2] if len(job.PT) > 1 and len(job.PT[1]) > 2 else 0
                return rework_time + sum(job.PT[0][violated_stage:])

            if stage_idx is None:
                stage_idx = 0

            for stage in self.Stg:
                for mac in stage.Macs:
                    if mac.Current_Job is job:
                        current_remaining = max(
                            0, mac.Current_Job_Completion_Time - self.Simu_Time
                        )
                        return current_remaining + sum(job.PT[0][stage_idx + 1:])

            return sum(job.PT[0][stage_idx:])

        def qtl_active_jobs(entries, stage_indices):
            return [
                job
                for job, stage_idx, in_rework in entries
                if not in_rework and stage_idx in stage_indices and job.isReworked == 0
            ]

        def qtl_slack(jobs, qtl_idx):
            if not jobs:
                return 1.0
            return min(qtl_slack_ratios(jobs, qtl_idx))

        def qtl_avg_waiting_ratio(jobs, qtl_idx):
            if not jobs:
                return 0.0
            return statistics.mean(qtl_waiting_ratios(jobs, qtl_idx))

        def qtl_slack_ratios(jobs, qtl_idx):
            ratios = []
            for job in jobs:
                qtl = max(1, job.QTL[qtl_idx])
                ratios.append((qtl - job.QTL_Waiting_Time[qtl_idx]) / qtl)
            return ratios

        def qtl_waiting_ratios(jobs, qtl_idx):
            ratios = []
            for job in jobs:
                qtl = max(1, job.QTL[qtl_idx])
                ratios.append(job.QTL_Waiting_Time[qtl_idx] / qtl)
            return ratios

        def clip01(value):
            return max(0.0, min(1.0, value))

        def normalized_std(values, norm=1.0):
            if not values:
                return 0.0
            return clip01(statistics.pstdev(values) / max(1e-9, norm))

        entries = active_entries()
        active_jobs = [job for job, _, _ in entries]
        active_count = len(active_jobs)

        # g1. QL: average queue length over all stages
        avg_queue_length = sum(len(stage.Queue) for stage in self.Stg) / max(1, stage_count)
        state.append(avg_queue_length / machines_per_stage)

        # g2. Idle: ratio of idle machines over all machines
        idle_ratio = sum(
            1 for stage in self.Stg for mac in stage.Macs if mac.Current_State == 0
        ) / max(1, stage_count * machines_per_stage)
        state.append(idle_ratio)

        # g3. Comp: ratio of completed jobs
        state.append(len(self.Dummy_Queue) / max(1, self.JN))

        # g4. RPT: average remaining processing time, normalized by average route time
        route_times = []
        for job in self.Jobs_List:
            rework_time = job.PT[1][2] if len(job.PT) > 1 and len(job.PT[1]) > 2 else 0
            route_times.append(sum(job.PT[0]) + rework_time)
        route_norm = max(1.0, statistics.mean(route_times)) if route_times else 1.0
        remaining_times = [
            remaining_processing_time(job, stage_idx, in_rework)
            for job, stage_idx, in_rework in entries
        ]
        avg_remaining_pt = statistics.mean(remaining_times) if remaining_times else 0.0
        state.append(avg_remaining_pt / route_norm)

        # g5. BNUtil: average utilization of bottleneck stages
        if self.Stg and self.Jobs_List:
            avg_stage_pt = [
                statistics.mean(job.PT[0][stage_idx] for job in self.Jobs_List)
                for stage_idx in range(stage_count)
            ]
            max_stage_pt = max(avg_stage_pt)
            bottleneck_stages = [
                idx for idx, value in enumerate(avg_stage_pt) if value == max_stage_pt
            ]
            bottleneck_util = statistics.mean(
                sum(1 for mac in self.Stg[idx].Macs if mac.Current_State != 0)
                / machines_per_stage
                for idx in bottleneck_stages
            )
        else:
            bottleneck_util = 0.0
        state.append(bottleneck_util)

        # g6. TardyRatio: ratio of active jobs already past due date
        tardy_ratio = (
            sum(1 for job in active_jobs if self.Simu_Time > job.Due_Date) / active_count
            if active_count > 0
            else 0.0
        )
        state.append(tardy_ratio)

        # g7. DueRisk: ratio of active jobs whose slack is negative
        due_risk = (
            sum(
                1
                for (job, _, _), remaining_time in zip(entries, remaining_times)
                if job.Due_Date - self.Simu_Time - remaining_time < 0
            )
            / active_count
            if active_count > 0
            else 0.0
        )
        state.append(due_risk)

        # g8. QTL24Slack: minimum remaining QTL slack for the 2-4 section
        qtl24_jobs = qtl_active_jobs(entries, {2, 3})
        state.append(qtl_slack(qtl24_jobs, 0))

        # g9. QTL35Slack: minimum remaining QTL slack for the 3-5 section
        qtl35_jobs = qtl_active_jobs(entries, {3, 4})
        state.append(qtl_slack(qtl35_jobs, 1))

        # g10. QTLEligibleRatio: ratio of active jobs still eligible for QTL checks
        qtl_eligible_ratio = (
            sum(1 for job in active_jobs if job.isReworked == 0) / active_count
            if active_count > 0
            else 0.0
        )
        state.append(qtl_eligible_ratio)

        # g11. QTL24AvgWT: average QTL waiting ratio for the 2-4 section
        state.append(qtl_avg_waiting_ratio(qtl24_jobs, 0))

        # g12. QTL35AvgWT: average QTL waiting ratio for the 3-5 section
        state.append(qtl_avg_waiting_ratio(qtl35_jobs, 1))

        # g13. ReEntWIP: ratio of re-entered jobs among WIP in stages 1-5
        reent_denominator = [
            job
            for job, stage_idx, in_rework in entries
            if not in_rework and stage_idx is not None and 0 <= stage_idx <= 4
        ]
        reent_wip = (
            sum(1 for job in reent_denominator if job.isReentered == 1)
            / len(reent_denominator)
            if reent_denominator
            else 0.0
        )
        state.append(reent_wip)

        # g14. DownstreamLoad: ratio of remaining work assigned to downstream stages
        denominator = 0
        numerator = 0
        downstream_start = 5
        for job, stage_idx, in_rework in entries:
            if in_rework:
                violated_stage = getattr(job, "Violated_QTL", 0)
                rework_time = job.PT[1][2] if len(job.PT) > 1 and len(job.PT[1]) > 2 else 0
                remaining_stages = range(violated_stage, stage_count)
                denominator += rework_time + sum(job.PT[0][idx] for idx in remaining_stages)
                numerator += sum(
                    job.PT[0][idx] for idx in remaining_stages if idx >= downstream_start
                )
            else:
                start_idx = 0 if stage_idx is None else stage_idx
                remaining_stages = range(start_idx, stage_count)
                denominator += sum(job.PT[0][idx] for idx in remaining_stages)
                numerator += sum(
                    job.PT[0][idx] for idx in remaining_stages if idx >= downstream_start
                )
        state.append(numerator / denominator if denominator > 0 else 0.0)

        # g15. MNorm: normalized number of stages
        m_max = max(max(Stages), stage_count) if Stages else stage_count
        state.append(stage_count / max(1, m_max))

        # g16. QueueStd: normalized standard deviation of stage queue lengths
        queue_lengths = [len(stage.Queue) for stage in self.Stg]
        state.append(normalized_std(queue_lengths, machines_per_stage))

        # g17. UtilStd: normalized standard deviation of current stage busy ratios
        stage_busy_ratios = [
            sum(1 for mac in stage.Macs if mac.Current_State != 0)
            / max(1, len(stage.Macs))
            for stage in self.Stg
        ]
        state.append(normalized_std(stage_busy_ratios, 0.5))

        # g18. RemainingPTStd: normalized standard deviation of remaining processing times
        state.append(normalized_std(remaining_times, route_norm))

        # g19. DueSlackStd: normalized standard deviation of due slack ratios
        due_slack_ratios = [
            (job.Due_Date - self.Simu_Time - remaining_time) / route_norm
            for (job, _, _), remaining_time in zip(entries, remaining_times)
        ]
        state.append(normalized_std(due_slack_ratios, 1.0))

        # g20. QTLSlackStd: normalized standard deviation of QTL slack ratios
        qtl_slack_ratio_values = (
            qtl_slack_ratios(qtl24_jobs, 0) + qtl_slack_ratios(qtl35_jobs, 1)
        )
        state.append(normalized_std(qtl_slack_ratio_values, 1.0))

        # g21. WaitingRatioStd: normalized standard deviation of QTL waiting ratios
        qtl_waiting_ratio_values = (
            qtl_waiting_ratios(qtl24_jobs, 0) + qtl_waiting_ratios(qtl35_jobs, 1)
        )
        state.append(normalized_std(qtl_waiting_ratio_values, 1.0))

        return state

    def get_result(self):
        makespan = self.Simu_Time
        total_tardy_time = self.total_tardiness()
        stage_utils = self.stage_utilizations()
        return {
            "time": self.Simu_Time,
            "makespan": makespan,
            "completed_jobs": len(self.Dummy_Queue),
            "total_tardiness": self.total_tardiness(),
            "total_tardy_time": total_tardy_time,
            "tardy_jobs": self.tardy_jobs(),
            "total_flow_time": self.total_flow_time(),
            "utilization": self.utilization(),
            "stage_utils_cleaned": stage_utils,
        }

    def total_tardiness(self):
        return sum(max(0, job.Completion_Time[-1] - job.Due_Date) for job in self.Dummy_Queue)

    def tardy_jobs(self):
        return sum(1 for job in self.Dummy_Queue if job.Completion_Time[-1] > job.Due_Date)

    def total_flow_time(self):
        return sum(job.Completion_Time[-1] - job.Arrival_Time for job in self.Dummy_Queue)

    def stage_utilizations(self):
        if self.Simu_Time <= 0:
            return [0.0 for _ in self.Stg]
        return [
            100.0
            * sum(mac.Busy_Time for mac in stage.Macs)
            / max(1, self.Simu_Time * len(stage.Macs))
            for stage in self.Stg
        ]

    def utilization(self):
        if self.Simu_Time <= 0:
            return 0.0
        total_busy = sum(mac.Busy_Time for stage in self.Stg for mac in stage.Macs)
        total_capacity = self.Simu_Time * sum(len(stage.Macs) for stage in self.Stg)
        return 100.0 * total_busy / max(1, total_capacity)


class FixedRuleAgent:
    def __init__(self, action=0):
        self.action = action

    def select_action(self, state):
        return self.action


def get_baseline_result(input_path, gp_rules):
    cache_key = (input_path, tuple(gp_rules))
    if cache_key in BASELINE_RESULT_CACHE:
        return BASELINE_RESULT_CACHE[cache_key]

    baseline_sim = StepwiseSimulator(input_path, gp_rules)
    baseline_agent = FixedRuleAgent(action=0)
    done = False
    while not done:
        _, done, _ = baseline_sim.step(baseline_agent, store_transitions=False)

    baseline_result = baseline_sim.get_result()
    BASELINE_RESULT_CACHE[cache_key] = baseline_result
    return baseline_result


# ======================== 10. Dispatching Env ========================
class DispatchingEnv:
    def __init__(self, data_path, gp_rules=None, total_episodes=1000):
        self.data_path = data_path
        self.gp_rules = gp_rules if gp_rules is not None else GP_Rules
        self.total_episodes = total_episodes
        self.episode = 0
        self.sim = StepwiseSimulator(self.data_path, self.gp_rules)
        self.state_dim = 21
        self.action_dim = len(self.gp_rules)
        self.alpha = 1.0
        self.prev_info = None
        self.initial_info = None
        self.terminal_reward_applied = False

    def reset(self, episode=0):
        self.episode = episode
        self.alpha = self.reward_alpha()
        self.terminal_reward_applied = False

        base_result = get_baseline_result(self.data_path, self.gp_rules)
        self.initial_info = {
            "makespan": base_result["makespan"],
            "utilization": base_result["utilization"],
            "tardy_jobs": base_result["tardy_jobs"],
            "total_tardy_time": base_result["total_tardy_time"],
        }
        self.prev_info = {
            "makespan": 0.0,
            "utilization": 0.0,
            "tardy_jobs": 0,
            "total_tardy_time": 0.0,
        }

        self.sim.reset()
        return self.sim.get_state()

    def step(self, agent=None, store_transitions=True):
        event, done, stored_transitions = self.sim.step(
            agent,
            store_transitions=store_transitions,
            reward_fn=self.calculate_transition_reward,
        )
        next_state = self.sim.get_state()
        info = self.sim.get_result()
        info["stored_transitions"] = stored_transitions

        if done and not self.terminal_reward_applied:
            terminal_reward = self.calculate_terminal_reward(info)
            if agent is not None and store_transitions:
                agent.add_reward_to_last_transition(
                    terminal_reward,
                    next_state=next_state,
                    done=True,
                )
            self.sim.last_step_reward_sum += terminal_reward
            self.sim.last_step_reward_count += 1
            self.terminal_reward_applied = True

        if event:
            if self.sim.last_step_reward_count > 0:
                reward = self.sim.last_step_reward_sum
            else:
                reward = self.calculate_transition_reward(info, done)
            return next_state, reward, done, info
        else:
            return next_state, 0.0, done, info

    def reward_alpha(self):
        if self.total_episodes <= 1:
            return 0.0
        progress = (self.episode - 1) / (self.total_episodes - 1)
        return max(0.0, min(1.0, 1.0 - progress))

    def calculate_transition_reward(self, transition_info, done):
        info = self.sim.get_result() # 현재 DQN 시뮬레이션 결과
        reward = -0.05 if not done else 0.0

        self.prev_info = {
            "makespan": info["makespan"],
            "utilization": info["utilization"],
            "tardy_jobs": info["tardy_jobs"],
            "total_tardy_time": info["total_tardy_time"],
        }
        self.sim.last_step_reward_sum += reward
        self.sim.last_step_reward_count += 1
        return reward

    def calculate_terminal_reward(self, info):
        baseline_tardy = max(1e-9, self.initial_info["total_tardy_time"])
        return -float(info["total_tardy_time"] / baseline_tardy)


# ======================== 11. Training / Test ========================
def find_hfs_instances(folder):
    patterns = [
        os.path.join(folder, "Job*_instance*.txt"),
        os.path.join(folder, "*.txt"),
    ]
    instances = []
    for pattern in patterns:
        instances.extend(glob.glob(pattern))
    return sorted(set(instances))


def load_hfs_train_test_instances(base_path="adj_Data"):
    train_folder = os.path.join(base_path, "train")
    test_folder = os.path.join(base_path, "test")

    if os.path.isdir(train_folder):
        train_instances = find_hfs_instances(train_folder)
    else:
        train_instances = find_hfs_instances(base_path)

    if os.path.isdir(test_folder):
        test_instances_all = find_hfs_instances(test_folder)
    else:
        test_instances_all = train_instances[:]

    return train_instances, test_instances_all


def excel_col_name(index):
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def excel_cell(ref, value=None, style=None, formula=None):
    style_attr = f' s="{style}"' if style is not None else ""
    if formula is not None:
        cached = "" if value is None else f"<v>{value}</v>"
        return f'<c r="{ref}"{style_attr}><f>{escape(formula)}</f>{cached}</c>'
    if value is None:
        return f'<c r="{ref}"{style_attr}/>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"{style_attr}><v>{value}</v></c>'
    return f'<c r="{ref}"{style_attr} t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def excel_row(row_idx, values, styles=None, formulas=None):
    styles = styles or {}
    formulas = formulas or {}
    cells = []
    for col_idx, value in enumerate(values, start=1):
        ref = f"{excel_col_name(col_idx)}{row_idx}"
        cells.append(
            excel_cell(
                ref,
                value=value,
                style=styles.get(col_idx),
                formula=formulas.get(col_idx),
            )
        )
    return f'<row r="{row_idx}">{"".join(cells)}</row>'


def percent_change(new_value, old_value):
    if old_value == 0:
        return 0.0
    return (new_value - old_value) * 100.0 / old_value


def get_job_label_from_summary(summary_rows):
    job_counts = sorted(
        {
            row["instance"].split("_", 1)[0]
            for row in summary_rows
            if row.get("instance")
        },
        key=lambda x: int(x) if x.isdigit() else x,
    )
    return "-".join(job_counts) if job_counts else "unknown"


def write_result_summary_xlsx(result_path, summary_rows):
    max_stage_count = max(
        (
            len(row["baseline"].get("stage_utils_cleaned", []))
            for row in summary_rows
        ),
        default=0,
    )
    stage_headers = [
        f"Stage {stage_idx + 1} Util(%)"
        for stage_idx in range(max_stage_count)
    ]
    headers = [
        "Instance",
        "Method",
        "Makespan",
        "Util(%)",
        "Tardy Jobs",
        "Tardy Time",
        "CPU Time(sec)",
    ] + stage_headers
    rows = [excel_row(1, headers, styles={i: 1 for i in range(1, len(headers) + 1)})]
    row_idx = 2

    for item in summary_rows:
        instance = item["instance"]
        baseline = item["baseline"]
        dqn = item["dqn"]
        baseline_stage_utils = baseline.get("stage_utils_cleaned", [])
        dqn_stage_utils = dqn.get("stage_utils_cleaned", [])

        rows.append(
            excel_row(
                row_idx,
                [
                    instance,
                    "baseline",
                    baseline["makespan"],
                    baseline["utilization"],
                    baseline["tardy_jobs"],
                    baseline["total_tardy_time"],
                    baseline["cpu_time"],
                ] + [
                    round(baseline_stage_utils[stage_idx], 2)
                    if stage_idx < len(baseline_stage_utils)
                    else ""
                    for stage_idx in range(max_stage_count)
                ],
            )
        )
        baseline_row = row_idx
        row_idx += 1

        rows.append(
            excel_row(
                row_idx,
                [
                    "",
                    "DQNAgent",
                    dqn["makespan"],
                    dqn["utilization"],
                    dqn["tardy_jobs"],
                    dqn["total_tardy_time"],
                    dqn["cpu_time"],
                ] + [
                    round(dqn_stage_utils[stage_idx], 2)
                    if stage_idx < len(dqn_stage_utils)
                    else ""
                    for stage_idx in range(max_stage_count)
                ],
            )
        )
        dqn_row = row_idx
        row_idx += 1

        stage_change_values = [
            percent_change(
                dqn_stage_utils[stage_idx],
                baseline_stage_utils[stage_idx],
            )
            if (
                stage_idx < len(dqn_stage_utils)
                and stage_idx < len(baseline_stage_utils)
            )
            else ""
            for stage_idx in range(max_stage_count)
        ]
        stage_change_styles = {
            8 + stage_idx: 2
            for stage_idx in range(max_stage_count)
        }
        stage_change_formulas = {
            8 + stage_idx: (
                f"IFERROR(({excel_col_name(8 + stage_idx)}{dqn_row}-"
                f"{excel_col_name(8 + stage_idx)}{baseline_row})*100/"
                f"{excel_col_name(8 + stage_idx)}{baseline_row},0)"
            )
            for stage_idx in range(max_stage_count)
        }

        rows.append(
            excel_row(
                row_idx,
                [
                    "",
                    "baseline vs DQN change(%)",
                    percent_change(dqn["makespan"], baseline["makespan"]),
                    percent_change(dqn["utilization"], baseline["utilization"]),
                    "",
                    percent_change(dqn["total_tardy_time"], baseline["total_tardy_time"]),
                    "",
                    "",
                ] + stage_change_values,
                styles={3: 2, 4: 2, 6: 2, **stage_change_styles},
                formulas={
                    3: f"IFERROR((C{dqn_row}-C{baseline_row})*100/C{baseline_row},0)",
                    4: f"IFERROR((D{dqn_row}-D{baseline_row})*100/D{baseline_row},0)",
                    6: f"IFERROR((F{dqn_row}-F{baseline_row})*100/F{baseline_row},0)",
                    **stage_change_formulas,
                },
            )
        )
        row_idx += 1

    last_col_name = excel_col_name(len(headers))
    sheet_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<dimension ref="A1:{last_col_name}{row_idx - 1}"/>
<sheetViews><sheetView workbookViewId="0"/></sheetViews>
<sheetFormatPr defaultRowHeight="16"/>
<cols>
<col min="1" max="1" width="18" customWidth="1"/>
<col min="2" max="2" width="28" customWidth="1"/>
<col min="3" max="7" width="15" customWidth="1"/>
<col min="8" max="{len(headers)}" width="17" customWidth="1"/>
</cols>
<sheetData>{"".join(rows)}</sheetData>
<pageMargins left="0.7" right="0.7" top="0.75" bottom="0.75" header="0.3" footer="0.3"/>
</worksheet>'''

    styles_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>
<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="3">
<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0"/>
<xf numFmtId="2" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>
</cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>'''

    workbook_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="Summary" sheetId="1" r:id="rId1"/></sheets>
</workbook>'''

    workbook_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>'''

    root_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>'''

    content_types = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>'''

    now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    core_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<dc:creator>HFS_OQTL-RL</dc:creator><cp:lastModifiedBy>HFS_OQTL-RL</cp:lastModifiedBy>
<dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified>
</cp:coreProperties>'''
    app_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
<Application>HFS_OQTL-RL</Application></Properties>'''

    os.makedirs(os.path.dirname(result_path), exist_ok=True)
    with zipfile.ZipFile(result_path, "w", zipfile.ZIP_DEFLATED) as xlsx:
        xlsx.writestr("[Content_Types].xml", content_types)
        xlsx.writestr("_rels/.rels", root_rels)
        xlsx.writestr("xl/workbook.xml", workbook_xml)
        xlsx.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        xlsx.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        xlsx.writestr("xl/styles.xml", styles_xml)
        xlsx.writestr("docProps/core.xml", core_xml)
        xlsx.writestr("docProps/app.xml", app_xml)


def run_hfs_dqn_experiment(base_path="adj_Data", total_episodes=1000, condition="hfs"):
    if torch is None:
        raise ImportError("PyTorch is required to train DQNAgent.")

    print(f"\n {condition.upper()} training start\n")

    # ========== A. Data loading ==========
    train_instances, test_instances_all = load_hfs_train_test_instances(base_path)
    test_instances = test_instances_all

    if not train_instances:
        raise FileNotFoundError("No training instances were found.") #흐
    if not test_instances:
        raise FileNotFoundError("No test instances were found.")

    TOTAL_EPISODES = total_episodes
    output_dir = os.path.join("resultRL7", condition)
    os.makedirs(output_dir, exist_ok=True)
    train_reward_path = os.path.join(output_dir, f"train_rewards_{condition}.csv")
    model_path = os.path.join(output_dir, f"dqn_model_{condition}.pth")
    reward_plot_path = os.path.join(output_dir, f"reward_plot_{condition}.png")
    objective_plot_path = os.path.join(output_dir, f"objective_plot_{condition}.png")
    objective_per_job_plot_path = os.path.join(
        output_dir, f"objective_per_job_plot_{condition}.png"
    )
    objective_ratio_plot_path = os.path.join(
        output_dir, f"objective_ratio_plot_{condition}.png"
    )
    train_loss_plot_path = os.path.join(output_dir, f"train_loss_{condition}.png")
    checkpoint_dir = os.path.join(output_dir, "checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)

    def switch_locked_reward_log(locked_path):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        fallback_path = os.path.join(
            output_dir,
            f"train_rewards_{condition}_{timestamp}.csv",
        )
        print(
            f"Training reward log is locked: {locked_path}. "
            f"Writing to {fallback_path} instead."
        )
        return fallback_path

    def append_train_reward(line):
        nonlocal train_reward_path
        try:
            with open(train_reward_path, "a") as f:
                f.write(line)
        except PermissionError:
            train_reward_path = switch_locked_reward_log(train_reward_path)
            with open(train_reward_path, "a") as f:
                f.write(line)

    try:
        with open(train_reward_path, "a"):
            pass
    except PermissionError:
        train_reward_path = switch_locked_reward_log(train_reward_path)

    # ========== B. Environment and agent initialization ==========
    env_example = DispatchingEnv(train_instances[0])
    agent = DQNAgent(state_dim=env_example.state_dim, action_dim=env_example.action_dim)
    reward_history = []
    objective_history = []
    objective_per_job_history = []
    objective_ratio_history = []
    train_loss_history = []
    scheduler = HybridEpsilonScheduler(
        init=1.0,
        mid=0.1,
        final=0.01,
        total_episodes=TOTAL_EPISODES,
    )
    validation_interval = 50
    validation_count = min(8, len(train_instances))
    if validation_count <= 1:
        validation_instances = train_instances[:]
    else:
        validation_indices = np.linspace(
            0, len(train_instances) - 1, validation_count, dtype=int
        )
        validation_instances = [train_instances[idx] for idx in validation_indices]
    best_validation_ratio = float("inf")
    best_model_path = model_path

    def evaluate_validation_policy():
        old_epsilon = agent.epsilon
        agent.epsilon = 0.0
        ratios = []
        tardy_times = []
        rewards = []
        try:
            for validation_path in validation_instances:
                val_env = DispatchingEnv(validation_path, total_episodes=TOTAL_EPISODES)
                val_env.reset()
                done = False
                val_reward = 0.0
                while not done:
                    _, reward, done, val_info = val_env.step(
                        agent, store_transitions=False
                    )
                    val_reward += reward
                baseline_info = get_baseline_result(validation_path, GP_Rules)
                baseline_tardy = max(1e-9, baseline_info["total_tardy_time"])
                ratios.append(val_info["total_tardy_time"] / baseline_tardy)
                tardy_times.append(val_info["total_tardy_time"])
                rewards.append(val_reward)
        finally:
            agent.epsilon = old_epsilon

        return {
            "avg_ratio": statistics.mean(ratios) if ratios else float("inf"),
            "avg_tardy_time": statistics.mean(tardy_times) if tardy_times else float("inf"),
            "avg_reward": statistics.mean(rewards) if rewards else 0.0,
        }

    # ========== C. Training ==========
    for episode in range(1, TOTAL_EPISODES + 1):
        env = DispatchingEnv(random.choice(train_instances), total_episodes=TOTAL_EPISODES)
        state = env.reset(episode=episode)
        done = False
        total_reward = 0

        epsilon_start = agent.epsilon  # 시작값 저장
        while not done:
            next_state, reward, done, info = env.step(agent)
            for _ in range(info["stored_transitions"]):
                agent.update()
                if hasattr(agent, "last_loss") and agent.last_loss is not None:
                    train_loss_history.append(agent.last_loss.item())
            state = next_state
            total_reward += reward

        reward_per_job = total_reward / max(1, env.sim.JN)
        objective_value = info["total_tardy_time"]
        objective_per_job = objective_value / max(1, env.sim.JN)
        baseline_objective = max(1e-9, env.initial_info["total_tardy_time"])
        objective_ratio = objective_value / baseline_objective

        # decay 이후 epsilon 저장
        agent.epsilon = scheduler.update(reward_per_job)
        epsilon_end = agent.epsilon
        reward_history.append(reward_per_job)
        objective_history.append(objective_value)
        objective_per_job_history.append(objective_per_job)
        objective_ratio_history.append(objective_ratio)

        # validation 평가 (greedy policy)
        if episode % validation_interval == 0:
            validation_info = evaluate_validation_policy()
            checkpoint_path = os.path.join(
                checkpoint_dir,
                (
                    f"dqn_model_{condition}_ep{episode:05d}_"
                    f"ratio{validation_info['avg_ratio']:.4f}.pth"
                ),
            )
            torch.save(agent.q_net.state_dict(), checkpoint_path)
            if validation_info["avg_ratio"] < best_validation_ratio:
                best_validation_ratio = validation_info["avg_ratio"]
                agent.target_net.load_state_dict(agent.q_net.state_dict())
                best_model_path = os.path.join(
                    checkpoint_dir, f"dqn_model_{condition}_best.pth"
                )
                torch.save(agent.q_net.state_dict(), best_model_path)
            print(
                f"[Validation] Episode {episode}, "
                f"Avg Reward: {validation_info['avg_reward']:.4f}, "
                f"Avg Tardy: {validation_info['avg_tardy_time']:.2f}, "
                f"Avg Ratio: {validation_info['avg_ratio']:.4f}, "
                f"Saved: {checkpoint_path}"
            )

        used_instance = os.path.basename(env.sim.input_path)
        append_train_reward(
            f"{episode},{used_instance},{total_reward:.4f},"
            f"{reward_per_job:.4f},{objective_value:.4f},"
            f"{objective_per_job:.4f},{objective_ratio:.6f},"
            f"{epsilon_start:.4f},{epsilon_end:.4f}\n"
        )

    torch.save(agent.q_net.state_dict(), model_path)
    if best_model_path != model_path:
        print(f"Final model saved: {model_path}")
        print(
            f"Best validation model: {best_model_path} "
            f"(avg ratio={best_validation_ratio:.4f})"
        )
    else:
        print(f"Model saved: {model_path}")

    # ========== D. 시각화==========
    def moving_average(x, w=20):
        if len(x) < w:
            return np.array(x)
        return np.convolve(x, np.ones(w), "valid") / w

    plt.figure(figsize=(10, 5))
    plt.plot(reward_history, label="Reward (used in training)", color="blue")
    plt.plot(moving_average(reward_history), label="Moving Avg", color="orange")
    plt.xlabel("Episode")
    plt.ylabel("Reward (normalized)")
    plt.title(f"[{condition.upper()}] DQN Training Reward (Per-Job Normalized)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(reward_plot_path)
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.plot(objective_history, label="Objective: Total Tardy Time", color="red")
    plt.plot(moving_average(objective_history), label="Moving Avg", color="green")
    plt.xlabel("Episode")
    plt.ylabel("Total Tardy Time")
    plt.title(f"[{condition.upper()}] Training Objective (Total Tardy Time)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(objective_plot_path)
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.plot(
        objective_per_job_history,
        label="Objective: Tardy Time Per Job",
        color="purple",
    )
    plt.plot(moving_average(objective_per_job_history), label="Moving Avg", color="green")
    plt.xlabel("Episode")
    plt.ylabel("Tardy Time Per Job")
    plt.title(f"[{condition.upper()}] Training Objective (Tardy Time Per Job)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(objective_per_job_plot_path)
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.plot(
        objective_ratio_history,
        label="Objective / Baseline Objective",
        color="brown",
    )
    plt.plot(moving_average(objective_ratio_history), label="Moving Avg", color="green")
    plt.axhline(1.0, color="black", linestyle="--", linewidth=1, label="Baseline")
    plt.xlabel("Episode")
    plt.ylabel("DQN Total Tardy Time / Baseline Total Tardy Time")
    plt.title(f"[{condition.upper()}] Training Objective vs Baseline")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(objective_ratio_plot_path)
    plt.close()

    if train_loss_history:
        plt.figure(figsize=(10, 5))
        plt.plot(train_loss_history, label="Train Loss", color="orange")
        plt.xlabel("Training Step")
        plt.ylabel("Loss")
        plt.title(f"[{condition.upper()}] DQN Training Loss")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(train_loss_plot_path)
        plt.close()

    # ========== E. 테스트==========
    agent.epsilon = 0.0
    test_model_path = best_model_path if os.path.exists(best_model_path) else model_path
    agent.q_net.load_state_dict(torch.load(test_model_path))
    agent.target_net.load_state_dict(agent.q_net.state_dict())
    print(f"Testing with model: {test_model_path}")

    result_dir = output_dir

    def write_result_file(result_path, instance_path, info, cpu_time):
        with open(result_path, "w") as f:
            f.write(
                f"{'':<9}{'Instance':<22}{'Makespan':<10}{'Util(%)':<10}"
                f"{'Tardy Jobs':<13}{'Tardy Time':<13}"
                f"{'CPU Time(sec)':<15}Stage Utils\n"
            )
            f.write("-" * 105 + "\n")
            f.write(
                f"{'':<9}{os.path.basename(instance_path):<22}{info['makespan']:<10.2f}"
                f"{info['utilization']:<10.2f}{info['tardy_jobs']:<13}"
                f"{info['total_tardy_time']:<13.2f}{cpu_time:<15.2f}"
                f"{[round(x, 2) for x in info['stage_utils_cleaned']]}\n"
            )

    summary_rows = []

    for instance_path in test_instances:
        baseline_name = f"baseline_{os.path.basename(instance_path)}"
        baseline_path = os.path.join(result_dir, baseline_name)

        baseline_sim = StepwiseSimulator(instance_path, GP_Rules)
        baseline_agent = FixedRuleAgent(action=0)
        done = False
        baseline_start_time = time.time()
        while not done:
            _, done, _ = baseline_sim.step(baseline_agent, store_transitions=False)
        baseline_cpu_time = time.time() - baseline_start_time
        baseline_info = baseline_sim.get_result()
        write_result_file(baseline_path, instance_path, baseline_info, baseline_cpu_time)
        baseline_summary = baseline_info.copy()
        baseline_summary["cpu_time"] = baseline_cpu_time

        instance_name = f"{condition}_{os.path.basename(instance_path)}"
        result_path = os.path.join(result_dir, instance_name)

        env = DispatchingEnv(instance_path)
        state = env.reset()
        done = False
        total_reward = 0
        start_time = time.time()

        while not done:
            next_state, reward, done, info = env.step(agent, store_transitions=False)
            state = next_state
            total_reward += reward

        cpu_time = time.time() - start_time
        write_result_file(result_path, instance_path, info, cpu_time)
        dqn_summary = info.copy()
        dqn_summary["cpu_time"] = cpu_time
        summary_rows.append(
            {
                "instance": os.path.splitext(os.path.basename(instance_path))[0],
                "baseline": baseline_summary,
                "dqn": dqn_summary,
            }
        )

    job_label = get_job_label_from_summary(summary_rows)
    summary_path = os.path.join(result_dir, f"result_{TOTAL_EPISODES}_{job_label}.xlsx")
    write_result_summary_xlsx(summary_path, summary_rows)
    print(f"Summary saved: {summary_path}")

    print(f" {condition.upper()} test complete -> {result_dir}\n")
    return agent, reward_history, train_loss_history, objective_history


def run_hfs_dqn_test_only(
    model_path,
    base_path="adj_Data",
    condition="hfs",
    total_episodes=1350,
    output_dir=None,
):
    if torch is None:
        raise ImportError("PyTorch is required to test DQNAgent.")
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file was not found: {model_path}")

    _, test_instances = load_hfs_train_test_instances(base_path)
    if not test_instances:
        raise FileNotFoundError("No test instances were found.")

    result_dir = output_dir if output_dir is not None else os.path.join("resultRL7", condition)
    os.makedirs(result_dir, exist_ok=True)

    env_example = DispatchingEnv(test_instances[0])
    agent = DQNAgent(state_dim=env_example.state_dim, action_dim=env_example.action_dim)
    agent.epsilon = 0.0
    agent.q_net.load_state_dict(torch.load(model_path, map_location="cpu"))
    agent.target_net.load_state_dict(agent.q_net.state_dict())
    print(f"Testing with model: {model_path}")

    def write_result_file(result_path, instance_path, info, cpu_time):
        with open(result_path, "w") as f:
            f.write(
                f"{'':<9}{'Instance':<22}{'Makespan':<10}{'Util(%)':<10}"
                f"{'Tardy Jobs':<13}{'Tardy Time':<13}"
                f"{'CPU Time(sec)':<15}Stage Utils\n"
            )
            f.write("-" * 105 + "\n")
            f.write(
                f"{'':<9}{os.path.basename(instance_path):<22}{info['makespan']:<10.2f}"
                f"{info['utilization']:<10.2f}{info['tardy_jobs']:<13}"
                f"{info['total_tardy_time']:<13.2f}{cpu_time:<15.2f}"
                f"{[round(x, 2) for x in info['stage_utils_cleaned']]}\n"
            )

    summary_rows = []

    for instance_path in test_instances:
        baseline_name = f"baseline_{os.path.basename(instance_path)}"
        baseline_path = os.path.join(result_dir, baseline_name)

        baseline_sim = StepwiseSimulator(instance_path, GP_Rules)
        baseline_agent = FixedRuleAgent(action=0)
        done = False
        baseline_start_time = time.time()
        while not done:
            _, done, _ = baseline_sim.step(baseline_agent, store_transitions=False)
        baseline_cpu_time = time.time() - baseline_start_time
        baseline_info = baseline_sim.get_result()
        write_result_file(baseline_path, instance_path, baseline_info, baseline_cpu_time)
        baseline_summary = baseline_info.copy()
        baseline_summary["cpu_time"] = baseline_cpu_time

        instance_name = f"{condition}_{os.path.basename(instance_path)}"
        result_path = os.path.join(result_dir, instance_name)

        env = DispatchingEnv(instance_path)
        state = env.reset()
        done = False
        start_time = time.time()

        while not done:
            next_state, _, done, info = env.step(agent, store_transitions=False)
            state = next_state

        cpu_time = time.time() - start_time
        write_result_file(result_path, instance_path, info, cpu_time)
        dqn_summary = info.copy()
        dqn_summary["cpu_time"] = cpu_time
        summary_rows.append(
            {
                "instance": os.path.splitext(os.path.basename(instance_path))[0],
                "baseline": baseline_summary,
                "dqn": dqn_summary,
            }
        )

    job_label = get_job_label_from_summary(summary_rows)
    summary_path = os.path.join(result_dir, f"result_{total_episodes}_{job_label}.xlsx")
    write_result_summary_xlsx(summary_path, summary_rows)
    print(f"Summary saved: {summary_path}")
    print(f" {condition.upper()} test complete -> {result_dir}\n")
    return summary_path


# ========================  F. Smoke Test ========================
def run_random_smoke_test(instance_path="adj_Data/10_7_3_0.txt", max_steps=100000):
    env = DispatchingEnv(instance_path)
    state = env.reset()
    done = False
    steps = 0
    start = time.process_time()

    while not done and steps < max_steps:
        state, reward, done, info = env.step(agent=None)
        steps += 1

    elapsed = time.process_time() - start
    print("Smoke test finished")
    print("time_steps =", steps)
    print("done =", done)
    print("state =", state)
    print("reward =", reward)
    print("info =", info)
    print("cpu_time =", elapsed)


if __name__ == "__main__":
    run_hfs_dqn_experiment()

