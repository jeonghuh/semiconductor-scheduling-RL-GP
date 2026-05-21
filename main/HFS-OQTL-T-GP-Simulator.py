import random
import copy
import time
import math
import statistics
from random import randint

Jobs = [10,15]
Stages = [7]
Machines = [3]
TF = [0.6]
RDD = [0.5]
Rework_ST = [1]
Queue_T = [1]
Num_instance = 5 #데이터파일 돌릴 횟수(갯수)


Num_Generations = 20
Population_Size = 40

Minimum_Depth = 2
Maximum_Depth = 6
Prob_Crossover = 0.9
Prob_Mutation = 0.1

Function_Set = ['+', '-', '*', 'max', 'min']
Job_Terminal = ['Stg[i].Queue[k].Due_Date', 'Stg[i].Queue[k].PT[0][i]', 'Stg[i].Queue[k].Waiting_Time', 'Stg[i].Queue[k].Remaining_Time', 'Stg[i].Queue[k].Positive_Slack_Time', 'Stg[i].Queue[k].Negative_Slack_Time',
                'Stg[i].Queue[k].Current_Flow_Time', 'Stg[i].Queue[k].Remaining_Ops', 'Stg[i].Queue[k].Min_QTL', 'Stg[i].Queue[k].Avg_QTL', 'Stg[i].Queue[k].Next_PT', 'Stg[i].Queue[k].Min_PT', 'Stg[i].Queue[k].Avg_PT', 
                'Stg[i].Queue[k].Median_PT', 'Stg[i].Queue[k].Critical_Ratio']

class Job_T:
    def __init__(self, Index, Processing_Time, Queue_Time_Limits, DD, AT):
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
        self.Current_Job_Start_Time = Cur_J_ST #현재 잡의 시작 시간
        self.Current_Job_Completion_Time = Cur_J_CT #현재 잡 공정 끝난 시간
        self.Current_State = Cur_J_State #0 = (idle), 1 = (working) #잡의 상황
        self.Current_Job = Cur_J #현재 잡

class Stage_T:
    def __init__(self, M, Q):
        self.Macs = M #스테이지 별 머신
        self.Queue = Q #스테이지 별 큐

class RS_Machine_T:
    def __init__(self, Cur_J_ST, Cur_J_CT, Cur_J_State, Cur_J, list1):
        self.Current_R_Job_Completion_Time = Cur_J_CT
        self.Current_R_State = Cur_J_State #0 = (idle), 1 = (working)
        self.Current_R_Job = Cur_J
        self.Queue = list1


def Initialization(Func, J_Terminal): # Ramped-half-and-half method #랜덤하게 초기 트리 생성하는 
    GP_Job_Tree = [] 

    # Full method, Generate full trees at a given maximum depth 
    for i in range(int(Population_Size/2)): #절반은 풀매소드. 절반은 grow 매소드(언발란스한 트리로)
        Job_Tree = []
        Len = 1
        for j in range(Maximum_Depth):
            Job_SubTree = []
            for k in range(Len):
                if j < Maximum_Depth - 1: #맥시멈깊이 전까지
                    x = random.choice(Func) #기호들만 넣음
                    Job_SubTree.append(x)
                    
                else: #맥시멈 깊이가 되면
                    x = random.choice(J_Terminal) #맨 마지막 leaf node엔 비교할 job들 넣긔
                    Job_SubTree.append(x)

            Job_Tree.append(Job_SubTree)
            Len *= 2
        
        GP_Job_Tree.append(Job_Tree)
    
    # Grow method, Generate unbalanced trees that are much smaller than the full trees
    for i in range(Population_Size - int(Population_Size / 2)): #아 나눌때 숫자가 딱 안 떨어질 수 있어서 이렇게
        Job_Tree = []
        Len = 1
        
        for j in range(Maximum_Depth):
            Job_SubTree = []
            if j < Minimum_Depth: #일단 최소층까지는 기호수식 넣고
                for j in range(0, Len):
                    x = random.choice(Func)
                    Job_SubTree.append(x)
            
            else:
                for k in Job_Tree[-1]: #가장 방금 넣은 노드 데려와서
                    if k in J_Terminal: #이미 기호가 아니라 job으로 들어갔으면 (leaf node더 이어붙일수없으니)
                        for l in range(0, 2): #이건 아래랑 같은 로직으로 만들어두려고 이렇게 한듯
                            pass #그냥패스 
                    else:
                        y = random.randint(0, 1) #0혹은1 중 랜덤으로 선택
                                            
                        if y == 1 and j < Maximum_Depth-1: #1이 나오고, 아직 최대깊이 도달 안했다면
                            for z in range(0, 2):
                                x = random.choice(Func) #새 연산자(기호)를 자식으로 붙여서 밑으로 뻗어감
                                Job_SubTree.append(x)
                        else: #0이거나, 최대깊이에 도달했다면
                            for z in range(0, 2):
                                x = random.choice(J_Terminal) #job terminal붙여서 트리 성장 마무리 
                                Job_SubTree.append(x)
                
            Job_Tree.append(Job_SubTree)
            Len *= 2
        GP_Job_Tree.append(Job_Tree)

    return GP_Job_Tree

def flat_list(sentence):  
    rules = []
    for i in sentence:
        if type(i) == type(list()):
            rules += (flat_list(i))
        else:
            rules.append(i)
    return rules

def make_rule(tree):
    sentence = copy.deepcopy(tree)
    md = len(sentence)-1 # 트리의 가장 깊은 층(Maximum Depth) 인덱스

        # while md > 1: # 루트 노드 방향으로 한 층씩 올라오면서 반복
        # x= []
        
        # if len(sentence[md]) == 0: #현재 층 노드가 다 비워졌으면 
        #     del sentence[md]         #그 층을 지우고       
        #     md -= 1  #한 층 위로 올라감 
        #(((수정파트)))
    while md > 0: # 루트 노드(0층) 방향까지 끝까지 조립해야 하므로 > 0 으로 변경
        if len(sentence[md]) == 0: 
            del sentence[md]         
            md -= 1  
            continue # 층이 정상적으로 비워졌으면 다음 루프로 안전하게 넘김
              
        assembled = False # 무한 루프 방지용 플래그 #추가
        for i_x in range(0, len(sentence[md-1])):
            
            if sentence[md-1][i_x] in Function_Set: #윗층(md-1)에서 기호 있으면 
                if len(sentence[md]) < 2:
                    continue  # 피연산자가 충분하지 않으면 다음으로 넘어감(2개미만)
                
                a = sentence[md][0] #피연산자1 (왼쪽 자식)
                b = sentence[md-1][i_x] #연산자 (부모기호)
                c = sentence[md][1] #피연산자2 (오른쪽 자식)

                if (b == '-' or b == 'min' or b == 'max') and a == c: #만약에 피연산자 같으면
                    while a == c:
                        c = random.choice(Job_Terminal) #랜덤하게 바꿔줌
                        sentence[md][1] = c

                x = []
                x.append("(")
                x.append(sentence[md].pop(0)) #그렇게 뽑아낸 애들을 pop(리스트에서 제거)해서    
                x.append(sentence[md-1].pop(i_x)) #괄호로 묶은 x리스트에 넣어줌
                x.append(sentence[md].pop(0))
                x.append(")")  #최종 형태는 ['(', '피연산자1', '+' , '피연산자2' , ')' ] 이런식
                   
                # max min 처리
                for chk in range(0, len(x)):  
                    if x[chk] == "max" or x[chk] == "min":     #맥스,민은 위치 조정해줘야됨        
                        x[chk], x[chk-1] = x[chk-1], x[chk] #위치바꿔서
                        x.insert(chk, "(")
                        x.insert(chk+2, ",")
                        x.insert(chk+4, ")") #max(A,B)형태 되도록 변환
                sentence[md-1].insert(i_x, x) #조립된 수식을 부모자리에 삽입 
                # x = [] 
                assembled = True
                break
            #    del sentence[-1](애써 만든 문장 삭제..)
            #  
        # 만약 자식이 모자라는 등의 이유로 아무 수식도 조립하지 못했다면 (불완전 트리)
        # 무한 루프에 빠지지 않도록 강제로 해당 층을 파기하고 위로 올라감
        if not assembled:
            del sentence[md]
            md -= 1

    # del sentence[-1] # 수식을 날려먹고 IndexError를 유발하는 코드 제거
    
   #트리 밑바닥부터 괄호가 묶이면서 점점 위로 올라가, 결국 제일 꼭대기 층에는 거대한 하나의 수식 덩어리만 남게 됨
    r_rule = "".join(flat_list(sentence)) # 중첩된 리스트를 풀고 1줄 문자열로 결합
    return r_rule

# Function to check if a level contains functions or terminals
def is_function_level(level):
    return all(node in Function_Set for node in level)

def is_terminal_level(level):
    return all(node in Job_Terminal for node in level)

def prec(tree):

    tree_prec = [[0]]
    for i in range(0, len(tree)-1):
        tmp = []
        for j in range(0, len(tree[i])):
            if tree[i][j] in Function_Set:
                for x in range(0, 2):
                    tmp.append(j)
        tree_prec.append(tmp)
    return tree_prec

# Crossover function ensuring valid crossover points
def Crossover(tree1, tree2):
    Child1 = copy.deepcopy(tree1)
    Child2 = copy.deepcopy(tree2)

    # try: #어느 나뭇가지 자를지 무작위선정
    #     level = random.randint(1, Minimum_Depth)
    #     index = random.randint(0, len(Child1[level])-1)
   
    # except:
    #     while Child2[level][index] in Job_Terminal or Child2[level][index] in Function_Set:
    #         level = random.randint(1, Minimum_Depth)
    #         index = random.randint(0, len(Child1[level]) - 1) #문법오류 수정
    #         # index = random.randint(0, len(Child1[level]-1)) 
    
# ***--수정코드--**
# 두 트리 모두에 존재하는 유효한 깊이(Level)를 선택
    max_level = min(len(Child1) - 1, len(Child2) - 1, Minimum_Depth)
    level = random.randint(1, max_level)
    
    # 두 트리의 해당 층(Level)에서 공통으로 존재하는 인덱스 범위 내에서 선택
    max_index = min(len(Child1[level]), len(Child2[level])) - 1
    index = random.randint(0, max_index)



    p_t1 = prec(Child1) #부모자식 관계 추적
    p_t2 = prec(Child2)

    # remove tree2 
    r_list = [] 
    r_list.append(index) #교차점 인뎃그를 삭제 리스트에 추가
    Child2[level][index] = Child1[level][index] #교차점의 머리(노드)를 child1의 걸로 교체
     ##위에서 index를 child2기준으로 뽑았기때문에 그 인덱스번호(그 층의 몇번째 노드)가 tree2에서는 없을 수 있음(언발란스 트리경우) -->index에러(즉시종료)
    for i in range(level+1, len(Child2)):
        r_list1 = []
        
        j  = 0
        while j < len(Child2[i]):
            if p_t2[i][j] in r_list: #부모가 잘린 노드라면
                r_list1.append(j) #자신도 삭제 리스트에 넣음
                p_t2[i][j] = "N" #지울거라는 표시(N)
                Child2[i][j] = "N" 
                
            j += 1
        r_list = copy.deepcopy(r_list1) #다음 층으로 삭제 대상 물림

    i = 0
    while i < len(Child2):
        j = 0
        while j < len(Child2[i]):
            if Child2[i][j] == "N":
                del Child2[i][j] # N표시 노드를 제거 
                del p_t2[i][j]
                j -= 1
            j += 1   
        i += 1             

    r_list = [index]

    co = [] # 추출한 새로운 가지(Subtree)를 담을 바구니
    # Tree1 ==> Tree2
    for i in range(level+1, len(Child1)):
        rt_list = []
        temp = []
        for j in range(0, len(Child1[i])):
            if p_t1[i][j] in r_list: # Child1의 교차점 아래에 달린 하위 노드라면
                rt_list.append(j) #교차점append
                temp.append(Child1[i][j]) # temp에 담아둠
        r_list = copy.deepcopy(rt_list)
        co.append(temp)    # 층별로 떼어낸 가지들을 co 배열에 저장-->나중에 트리 Child2 에 붙을거임

    # CO 붙이기
    r_list = [index]
    
    # for i in range(level, len(Child2)-1): ##원래꺼
    
    # Child1에서 가져온 서브트리(co)가 들어갈 공간이 Child2에 부족하면 빈 층을 새로 생성
    while len(Child2) <= level + len(co):
        Child2.append([])
        p_t2.append([]) #이렇게안하면 잘린 공간이 대치될 서브트리보다 공간이 작은 경우 문제 발생하기때문 
        
    # Child2의 기존 길이가 아닌, 이식할 서브트리(co)의 깊이만큼만 정확히 루프를 돎
    for i in range(level, level + len(co)):
        temp = []
        for j in range(0, len(Child2[i])):
            if j in r_list and Child2[i][j] in Function_Set:
                for x in range(0, 2):
                    p_t2[i+1].append(j)
                    p_t2[i+1].sort()
                    # if co[i-level]:  # 리스트가 비어 있지 않으면 pop 수행 (아래 줄로 수정)
                    if i - level < len(co) and co[i-level]:  # 인덱스 방어 및 리스트 비어있는지 확인(수정부분)
                        Child2[i+1].insert(p_t2[i+1].index(j), co[i-level].pop(-1))
              
                for k in range(0, len(p_t2[i+1])):
                    if p_t2[i+1][k] in r_list:
                        temp.append(k)
                
        r_list = copy.deepcopy(temp) 
    return Child2


def Mutation(tree1, tree2):
    Child1 = copy.deepcopy(tree1)
    Child2 = copy.deepcopy(tree2)

    #cross over함수랑 동일하게 수정
    # try: #자를 층과 넘버 노드(인덱스) select
    #     level = random.randint(1, Minimum_Depth)
    #     index = random.randint(0, len(Child1[level])-1)
   
    # except:
    #     while Child2[level][index] in Job_Terminal or Child2[level][index] in Function_Set:
    #         level = random.randint(1, Minimum_Depth)
    #         index = random.randint(0, len(Child1[level]) - 1)

    # 두 트리 모두에 존재하는 유효한 깊이(Level)를 선택
    max_level = min(len(Child1) - 1, len(Child2) - 1, Minimum_Depth)
    level = random.randint(1, max_level)
    
    # 두 트리의 해당 층(Level)에서 공통으로 존재하는 인덱스 범위 내에서 선택
    max_index = min(len(Child1[level]), len(Child2[level])) - 1
    index = random.randint(0, max_index)

    p_t1 = prec(Child1)
    p_t2 = prec(Child2)

    # remove tree2 
    r_list = []
    r_list.append(index)
    Child2[level][index] = Child1[level][index]
    
    for i in range(level+1, len(Child2)):
        r_list1 = []
        
        j  = 0 #tree 2부분 삭제
        while j < len(Child2[i]):
            if p_t2[i][j] in r_list:
                r_list1.append(j)
                p_t2[i][j] = "N"
                Child2[i][j] = "N"
                
            j += 1
        r_list = copy.deepcopy(r_list1)

    i = 0
    while i < len(Child2):
        j = 0
        while j < len(Child2[i]):
            if Child2[i][j] == "N":
                del Child2[i][j]
                del p_t2[i][j]
                j -= 1
            j += 1   
        i += 1             

    r_list = [index]

    co = []
    # Tree1 ==> Tree2
    for i in range(level+1, len(Child1)):
        rt_list = []
        temp = []
        for j in range(0, len(Child1[i])):
            if p_t1[i][j] in r_list:
                rt_list.append(j)
                temp.append(Child1[i][j])
        r_list = copy.deepcopy(rt_list)
        co.append(temp)   

    # CO 붙이기
    r_list = [index]
    for i in range(level, len(Child2)-1):
        temp = []
        for j in range(0, len(Child2[i])):
            if j in r_list and Child2[i][j] in Function_Set:
                for x in range(0, 2):
                    p_t2[i+1].append(j)
                    p_t2[i+1].sort()
                    if co[i-level]:  # 리스트가 비어 있지 않으면 pop 수행
                        Child2[i+1].insert(p_t2[i+1].index(j), co[i-level].pop(-1))
              
              
                for k in range(0, len(p_t2[i+1])):
                    if p_t2[i+1][k] in r_list:
                        temp.append(k)
                
        r_list = copy.deepcopy(temp) 
    return Child2

def Genetic_Programming(Stage_list, RMac, Num_Job, Rules, Pops):
    Generation = 0
    Population = copy.deepcopy(Pops)
    GP_Rules = copy.deepcopy(Rules)
    gp_start_time = time.time()

    while Generation < Num_Generations:
        # Initial population

        while True:
            # Evaluation (적합도 평가)
            Total_Tardiness = []
            for Rule in range(0, len(Population)):

                elapsed = time.time() - gp_start_time
                print(f"\r▶ 코드 실행 중... {elapsed:.1f}초 경과 (진행 세대: {Generation + 1}/{Num_Generations}, 규칙 평가: {Rule + 1}/{len(Population)})", end="", flush=True)

                Job_list = []

                for i in range(0, Num_Job):
                    Job_list.append(Job_T(i, Processing_Time[i], Queue_Time_Limits[i], Due_Date[i], Arrival_Time[i]))

                Total_Tardiness.append(Simulator(Stage_list, RMac, Job_list, GP_Rules, Rule)) 
                #모든 population을 시뮬레이터에 넣고 돌려봐서 total tardiness계산
            
            # TT(Total_Tardiness)와 Population을 함께 정렬하여 기존 적합도와 규칙의 매핑이 깨지는 것을 방지
            # Sorted_Population = [x for _, x in sorted(zip(Total_Tardiness, Population))]
            Sorted_TT, Sorted_Population = zip(*sorted(zip(Total_Tardiness, Population)))
            Sorted_TT = list(Sorted_TT)
            Sorted_Population = list(Sorted_Population)

            # 다음 generation에서 사용될 population 생성 (fitness function 높은 순서대로)
            if len(Population) > Population_Size: 
                # #Sorted_Population = [x for _, x in sorted(zip(Total_Tardiness, Population))]
                # Population = []
                # for i in range(Population_Size):
                #     Population.append(Sorted_Population[i])
                # a = 0
                Population = Sorted_Population[:Population_Size]
                Total_Tardiness = Sorted_TT[:Population_Size] # 슬라이싱 된 Population에 맞게 TT 값도 업데이트
            else:
                Population = Sorted_Population
                Total_Tardiness = Sorted_TT
            
            # Random Selection, Randmoly select 10 individuals
            List = []
            Value = []
            for i in range(10):
                List.append(randint(0,Population_Size))
            
            for i in List:
                Value.append(Total_Tardiness[i-1])
            
            ###### 10 개의 parent에 대해서 pairwise crossover ######
                
            # Genetic operators (Crossover, Mutation) 
            P_Crossover = random.random()
            P_Mutation = random.random()
            #Population = []

            # Crossover
            if P_Crossover < Prob_Crossover: #prob_cross는 위에서 0.9로 설정됨 
                for i in range(9):
                    Parent1 = Population[List[i]-1]
                    Parent2 = Population[List[i+1]-1]

                    Offspring1 = Crossover(Parent1, Parent2)
                    Offspring2 = Crossover(Parent2, Parent1)
                    Population.append(Offspring1)
                    Population.append(Offspring2)

            # Mutation
            if P_Mutation < Prob_Mutation:
                Max_Index = Value.index(max(Value)) # 성과가 나쁜 규칙
                Min_Index = Value.index(min(Value)) # 성과가 좋은 규칙
                Parent1 = Population[List[Min_Index] - 1] # Best parent
                Parent2 = Population[List[Max_Index] - 1] # Worst parent
                #뮤테이션 함수를 보면 tree1,tree2를 input으로 받아서 tree2의 부분을 1로 바꿔주기 때문에!! 
                Offspring3 = Mutation(Parent1, Parent2) #그래서 worst를 best값으로 바꿔치기해주려고 best, worst case를 집어넣음 
                Population.append(Offspring3)
            
            # Re-Initialization for diverse population
            Tmp_Pop = Initialization(Function_Set, Job_Terminal)
            
            Population += Tmp_Pop #처음개체(40)+crossover+mutation+re생성(40)/여기서 처음개체는 직전 루프에서 나온 상위 40개
            
            Current_Rules = []
            for i in range(0, len(Population)):
                Current_Rules.append(make_rule(Population[i]))
            GP_Rules = Current_Rules

            Generation += 1
            break #while true문 탈출 -->탈출하고 더이상 코드없으므로 본 while문 다시 반복(generation < num_gen 일 때까지)
    
    Total_Tardiness = []
    for Rule in range(0, len(GP_Rules)):
        Job_list = []

        for i in range(0, Num_Job):
            Job_list.append(Job_T(i, Processing_Time[i], Queue_Time_Limits[i], Due_Date[i], Arrival_Time[i]))

        Total_Tardiness.append(Simulator(Stage_list, RMac, Job_list, GP_Rules, Rule))
    
    Final_Population = [x for _, x in sorted(zip(Total_Tardiness, Population))]
    Final_Rules = [x for _, x in sorted(zip(Total_Tardiness, GP_Rules))]
    # Final_Rules = list(set(Final_Rules)) #set은 중복제거할 때 정렬순서 무작위로 박살냄 -->앞에서 소팅한 의미 상실
    Final_Rules = list(dict.fromkeys(Final_Rules)) # ((수정)) 정렬된 순서를 유지하면서 중복 제거 

    print(f"\n▶ GP 학습 완료! (총 소요 시간: {time.time() - gp_start_time:.1f}초)\n")

    return Final_Rules, Final_Population[:Population_Size] # 이전 세대의 우수 트리 정보를 반환

def Event_Excuter(Stg, R_Mac, Dummy_Queue, Simu_Time, Seq_R, r):
    Ev = 0
    Picked_Job = None
    Reworked_Job = None

    for i in range(len(Stg)):
        for j in range(len(Stg[i].Macs)):
            if Stg[i].Macs[j].Current_State == 0 and len(Stg[i].Queue) > 0: #머신이 쉬고있고, 현재 큐가 차있다면 
                Rule_Eval = []
                for k in range(len(Stg[i].Queue)): #큐에 있는 잡 하나씩 꺼내옴
                    Stg[i].Queue[k].Remaining_Time = 0 #그 스테이지의 있는 큐의 k번째 job. 남은 시간0으로 초기화
                    for l in range(i, len(Stg)): #현 스테이지부터 마지막 스테이지까지
                        Stg[i].Queue[k].Remaining_Time += Stg[i].Queue[k].PT[Stg[i].Queue[k].PrRoute][l] #잔여시간 싹 다 더하기
                    Stg[i].Queue[k].Current_Flow_Time = max(0, Simu_Time - Stg[i].Queue[k].Arrival_Time) #시스템에 머무른 시간. flow time계산 
                    Stg[i].Queue[k].Positive_Slack_Time = max(0, Stg[i].Queue[k].Due_Date - Simu_Time) #이미 due오바된 시간
                    Stg[i].Queue[k].Negative_Slack_Time = min(0, Stg[i].Queue[k].Due_Date - Simu_Time) #due까지 남은 시간 
                    Stg[i].Queue[k].Remaining_Ops = len(Stg) - i #남은 operation(스테이지) 수
                    Stg[i].Queue[k].Min_QTL = min(Stg[i].Queue[k].QTL)
                    Stg[i].Queue[k].Avg_QTL = statistics.mean(Stg[i].Queue[k].QTL)
                    if i != len(Stg) - 1: #마지막 스테이지가 아니라면 
                        Stg[i].Queue[k].Next_PT = Stg[i].Queue[k].PT[0][i+1] 
                    Stg[i].Queue[k].Min_PT = min(Stg[i].Queue[k].PT[0])
                    Stg[i].Queue[k].Avg_PT = statistics.mean(Stg[i].Queue[k].PT[0])
                    Stg[i].Queue[k].Median_PT = statistics.median(Stg[i].Queue[k].PT[0])
                    Stg[i].Queue[k].Critical_Ratio = (Stg[i].Queue[k].Due_Date - Stg[i].Queue[k].Current_Flow_Time) / Stg[i].Queue[k].PT[0][i]
                    Rule_Eval.append(eval(Seq_R[r])) #Seq_R: GP로 생성된 규칙 리스트, r:현재 평가중인 디스패칭 규칙 인덱스 
                
                Index = Rule_Eval.index(min(Rule_Eval)) #룰 상 결과값이 가장 적은(=우선순위가 높음) 잡을 선택 
                Picked_Job = Stg[i].Queue[Index]

                if Picked_Job != None:
                    if(Stg[i].Macs[j].Current_State == 0):
                        Stg[i].Macs[j].Current_Job = Picked_Job
                        Stg[i].Macs[j].Current_Job.isTerminated = 0
                        del[Stg[i].Queue[Index]]
                        Stg[i].Macs[j].Current_State = 1 #잡이 들어왔으니까 현상태 업데이트 
                        Stg[i].Macs[j].Current_Job.Start_Time.append(Simu_Time) #job객체에 시작시간 저장(여러공정 거쳐야해서 리스트형태)
                        Stg[i].Macs[j].Current_Job_Start_Time = Simu_Time #머신객체에 시작시간 저장
                        Stg[i].Macs[j].Current_Job_Completion_Time = Simu_Time + Stg[i].Macs[j].Current_Job.PT[0][i] #종료예상시간 업데이트 
                        Ev = 1 #잡이 머신에 들어왔으니 이벤트 발생  저장
            
            if Stg[i].Macs[j].Current_Job_Completion_Time == Simu_Time: #작업완료되면 

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
                    Stg[i+1].Queue.append(Stg[i].Macs[j].Current_Job) #다음 스테이지 큐로 보내버림 
                    Stg[i].Macs[j].Current_Job.isTerminated = 1
                    Stg[i].Macs[j].Current_Job = None
                    Stg[i].Macs[j].Current_State = 0
                    Stg[i].Macs[j].Current_Job_Start_Time = -1
                    Stg[i].Macs[j].Current_Job_Completion_Time = -1
                    Ev = 1
                
                elif (i == Stage_Number - 1) and (Stg[i].Macs[j].Current_Job.isReentered == 1): #마지막스테이지라면 
                    Stg[i].Macs[j].Current_Job.Completion_Time.append(Simu_Time)
                    Dummy_Queue.append(Stg[i].Macs[j].Current_Job) #종료되었으니까 더미로 보내버림 
                    Stg[i].Macs[j].Current_Job.isTerminated = 1 #공정완료
                    Stg[i].Macs[j].Current_Job = None #머신 초깋ㅘ 
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
                    if(Stg[i].Queue[k].isReworked == 0 and (Stg[i].Queue[k].QTL_Waiting_Time[0] > Stg[i].Queue[k].QTL[0])):
                        #재작업 안했었고, 대기시간이 큐타임 리밋보다 더 큰 경우
                        if Stg[i].Queue[k].isReentered==1: #재진입 이미 한 경우 
                            Stg[i].Queue[k].PrRoute = 1 #1 2 3 4 5 1 2 100 102 3 4 5 6 7~~
                        elif Stg[i].Queue[k].isReentered==0: #아직 재진입 안했으면 
                            Stg[i].Queue[k].PrRoute = 5 #1 2 100 102 3 4 5 1 2 3 4 5 6 7~
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
            
            # 재작업 기계(R_Mac) 특성에 맞춰 터미널(Terminal) 속성값 계산
            job.Remaining_Time = job.PT[1][2] # 재작업 소요 시간 추가
            v_stg = job.Violated_QTL
            for l in range(v_stg, len(Stg)):
                job.Remaining_Time += job.PT[0][l] # 실제 물리적 시뮬레이터가 PT[0]을 쓰므로 PT[0] 기준 합산
            job.Positive_Slack_Time = max(0, job.Due_Date - Simu_Time)
            job.Negative_Slack_Time = min(0, job.Due_Date - Simu_Time)
            job.Current_Flow_Time = Simu_Time # GP가 현재 시간을 인지하도록 동적 업데이트
            job.Remaining_Ops = len(Stg) - v_stg + 1
            job.Min_QTL = min(job.QTL)
            job.Avg_QTL = statistics.mean(job.QTL)
            job.Next_PT = job.PT[0][v_stg]
            
            # 재작업 시간을 포함한 실제 전체 공정 시간 리스트로 통계 재계산
            actual_pt_list = job.PT[0] + [job.PT[1][2]] 
            job.Min_PT = min(actual_pt_list)
            job.Avg_PT = statistics.mean(actual_pt_list)
            job.Median_PT = statistics.median(actual_pt_list)
            job.Critical_Ratio = (job.Due_Date - job.Current_Flow_Time) / job.PT[1][2]
            
            # GP 룰 문자열에서 메인 스테이지 참조 변수(Stg[i])를 R_Mac으로 치환하여 평가
            rule_str = Seq_R[r].replace('Stg[i]', 'R_Mac').replace('[0][i]', '[1][2]')
            Rule_Eval.append(eval(rule_str))
            
        Index = Rule_Eval.index(min(Rule_Eval))
        Reworked_Job = R_Mac.Queue[Index]
        
        if Reworked_Job != None:
            R_Mac.Current_R_Job = Reworked_Job
            R_Mac.Current_R_Job.Waiting_Time = 0
            R_Mac.Current_R_Job.QTL_Waiting_Time = [0, 0] # 재작업 시 QTL 대기시간 초기화
            R_Mac.Current_R_Job.isTerminated = 0
            # del[R_Mac.Queue[0]]
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

def Simulator(Stg, R_Mac, Jobs_List, Seq_R, r):
    Simu_Time = 0
    Dummy_Queue = []
    TT = 0 # Total Tardiness
    TFT = 0 # Total Flow Time
    JN = len(Jobs_List)

    #arrival time 순서대로 job sort
    Sorted_Jobs = sorted(Jobs_List, key=lambda x: (x.Arrival_Time, x.JobIndex))

    for i in range(len(Stg)):
        Stg[i].Queue = []
        for j in range(len(Stg[i].Macs)):
            Stg[i].Macs[j].Current_Job_Start_Time = -1
            Stg[i].Macs[j].Current_Job_Completion_Time = -1
            Stg[i].Macs[j].Current_State = 0
            Stg[i].Macs[j].Current_Job = None
    
    R_Mac.Queue = []
    R_Mac.Current_R_Job_Start_Time = -1
    R_Mac.Current_R_Job_Completion_Time = -1
    R_Mac.Current_R_State = 0
    R_Mac.Current_R_Job = None

    while True:

        #job 도착시간 == simulation time 될 때 큐에 넣어주기 (혹시 지나갔을 수 있으니까 <=으로 )
        while Sorted_Jobs and Sorted_Jobs[0].Arrival_Time <= Simu_Time:
            Stg[0].Queue.append(Sorted_Jobs.pop(0))
        
        Event = 1

        while Event != 0:
            # Return 되는 Event가 1이면 while문 계속 반복해서 event check!
            Event = Event_Excuter(Stg, R_Mac, Dummy_Queue, Simu_Time, Seq_R, r)

        # Stage 2-4 and Stage 3-5 사이에서 queue time limits check
        for i in range(2, 5):
            # for j in range(len(Stg[i].Macs)):
            #     if len(Stg[i].Queue) > 0:
            #         for k in range(0, len(Stg[i].Queue)):
            #             if Stg[i].Queue[k].isReworked == 0:
            #                 Stg[i].Queue[k].Waiting_Time += 1

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
# 최초 실행 시 사용할 초기 개체군(Population_Size) 생성
Global_Seq_Pop = Initialization(Function_Set, Job_Terminal)

for Job in Jobs:
    for Stage in Stages:
        for Machine in Machines:
            for RS in Rework_ST:
                for Q in Queue_T:
                    for T in TF:
                        for R in RDD:
                            for Instance in range(Num_instance):
                                File = open("adj_Data/" + str(Job) + "_" + str(Stage) + "_" + str(Machine) + "_" + str(Instance) + ".txt")
                                File_w = open("adj_c_Result/" + str(Job) + "_" + str(Stage) + "_" + str(Machine) + "_" + str(Instance) + ".txt", 'w')
                                
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
                                Process_Route_Number = 9 #RE-ENTRANCE추가해서 총 9개
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

                                # 직전 데이터 결과에서 살아남은 트리 구조(40개)를 그대로 가져와서 초기 개체로 사용 -> genetic_programming함수 안에서 이 초기개체+new 랜덤트리 생성해서 사용함
                                Seq_Pop = copy.deepcopy(Global_Seq_Pop)
                                
                                Seq_Rules = []

                                for i in range(0, Population_Size):
                                    Seq_Rules.append(make_rule(Seq_Pop[i]))

                                Final_GP_Rules, Global_Seq_Pop = Genetic_Programming(Stages_T, RS_Mac, Job_Number, Seq_Rules, Seq_Pop) #그때의 룰 같이 반환
                                #GP룰 파일을 읽어와서 하지 않는 이유는, 트리 구조를 가지고있어야 전이 등 할 수 있어서 그 구조 그대로 살리기 위해 스트링으로 변환하기 전 모양 그대로 따로 반환해 사용

                                End_Time = time.process_time()

                                Elapsed_Time = End_Time - Start_Time

                                File_w.write("Final_GP_Rules = ")
                                for i in range(len(Final_GP_Rules)):
                                    File_w.write(str(Final_GP_Rules[i]))
                                    File_w.write("\n")
                                    File_w.write("\n")
                                File_w.write("\n")
                                
                                File_w.write("Training CPU Time = ")
                                File_w.write(str(Elapsed_Time))
                                File_w.write("\n")
