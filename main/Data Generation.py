import random
import math

# Jobs = [5, 7, 10, 20, 40, 60, 80, 100]
# Stages = [5, 7, 10]
# Machines = [3, 5]
# Num_QTLs = 2
# TF = [0.2, 0.4, 0.6]
# RDD = [0.2, 0.5, 0.8]
# Rework_ST = [1, 2]
# Queue_T = [1, 2]

Jobs = [40,80,100,150,200]
Stages = [7]
Machines = [2,4]
Num_QTLs = 2
TF = [0.1]
RDD = [0.8]
Rework_ST = [1]
Queue_T = [1]
Num_instance=10

for Job in Jobs:
    for Stage in Stages:
        for Machine in Machines:
            for RS in Rework_ST:
                for Q in Queue_T:
                    for T in TF:
                        for R in RDD:
                            for Instance in range(Num_instance):
                                # File_w = open("C:/Users/admin/Desktop/Desktop/1-Research/0-Ph.D/0-Dissertation/1-Chapter 2/1-Code/0-Data Generation/0-Training Data/Surrogate_Model_5/Data_" + str(RS) + "_" + str(Q) + "/Data_" + str(T) + "_" + str(R) + "/Data_" + 
                                            # str(Job) + "_" + str(Stage) + "_" + str(Machine) + "_" + str(Instance) + ".txt", 'w')
                                File_w = open("newTest_Data/"+ str(Job) + "_" + str(Stage) + "_" + str(Machine) + "_" + str(Instance) + ".txt", 'w')
                                    
                                File_w.write(str(Job))
                                File_w.write("\n")
                                File_w.write(str(Stage))
                                File_w.write("\n")
                                File_w.write(str(Machine))
                                File_w.write("\n")
                                File_w.write(str(Num_QTLs))
                                File_w.write("\n")
                                File_w.write("\n")

                                # 각 job 별로 Arrival time
                                AT=[]
                                for i in range(Job):
                                    AT.append(random.randint(0, 30)) #랜덤하게 도착
                                    File_w.write(str (AT[i]))
                                    File_w.write("\n")
                                File_w.write("\n")


                                # 각 job 별로 각 stage에서의 processing time
                                PT = []
                                for i in range(Job):
                                    List = []
                                    for j in range(Stage):
                                        Tmp = random.randint(5,30)
                                        List.append(Tmp)
                                    PT.append(List)

                                
                                              
                                # 각 job 별로 Rework setup station에서의 rework setup time
                                RST = []
                                if RS == 1:
                                    for i in range(Job):
                                        Tmp = random.randint(5,20)
                                        RST.append(Tmp)
                                elif RS == 2:
                                    for i in range(Job):
                                        Tmp = random.randint(80,100)
                                        RST.append(Tmp)

                                # 각 job 별로 Queue time limits
                                QTLs = []
                                if Q == 1:
                                    for i in range(Job):
                                        List = []
                                        for j in range(Num_QTLs):
                                            # QTL은 2-4 & 3-5 사이에 존재
                                            Tmp = random.randint(5,20) + PT[i][2+j] # Original QTL에 QTL 사이에 존재하는 stage의 processing time 합
                                            List.append(Tmp)
                                            File_w.write(str(Tmp) + " ")
                                        QTLs.append(List)
                                        File_w.write("\n")
                                    File_w.write("\n")
                                
                                if Q == 2:
                                    for i in range(Job):
                                        List = []
                                        for j in range(Num_QTLs):
                                            # QTL은 2-4 & 3-5 사이에 존재
                                            Tmp = random.randint(80,100) + PT[i][2+j] # Original QTL에 QTL 사이에 존재하는 stage의 processing time 합
                                            List.append(Tmp)
                                            File_w.write(str(Tmp) + " ")
                                        QTLs.append(List)
                                        File_w.write("\n")
                                    File_w.write("\n")

                                # 각 job 별로 Due date
                                for i in range(Job):
                                    TPT = 0
                                    for j in range(Stage):

                                        # TPT += PT[i][j]
                                        if j < 5: # 1~5번째 스테이지(인덱스 0~4)는 재진입하므로 2배 반영
                                            TPT += PT[i][j] * 2
                                        else:
                                            TPT += PT[i][j]

                                    # Tmp = random.randint(round(TPT*(1-T-(R/2)/Stage), 0), round(TPT*(1-T+(R/2)/Stage), 0)) + AT[i] #+AT[i]통해 arrival만큼 추가해서 반영 
                                    Tmp = TPT*2 + random.randint(round(TPT*(1-T-(R/2)/Stage), 0), round(TPT*(1-T+(R/2)/Stage), 0)) + AT[i] # 도착 시간(AT) + 총 처리 시간(TPT) + 여유 시간(Slack Time)
                                    File_w.write(str(Tmp))
                                    File_w.write("\n")
                                File_w.write("\n")
                                
                                PR_Routes = []
                                Normal_Operations = []
                                for i in range(Stage):
                                    Normal_Operations.append(i+1)
                        
                                Rework_Operations = [100, 102, 103, 104]
                                # File_w.write(str('100 102 103 104'))
                                # File_w.write("\n")
                                # File_w.write("\n")
                                
                                List = []
                                List1 = []
                                List2 = []
                                List3 = []
                                List4 = []
                                List11 = []
                                List22 = []
                                List33 = []
                                List44 = []

                                #12345 + (re-entrance) rework
                                List = Normal_Operations[:5] + Normal_Operations #12345 12345~
                                List1 = Normal_Operations[:5]  + Normal_Operations[:2] + Rework_Operations[:2] + Normal_Operations[2:]
                                List2 = Normal_Operations[:5]  + Normal_Operations[:3] + Rework_Operations[:3] + Normal_Operations[3:]
                                List3 = Normal_Operations[:5]  + Normal_Operations[:3] + Rework_Operations[:1] + Rework_Operations[2:3] + Normal_Operations[3:]
                                List4 = Normal_Operations[:5]  + Normal_Operations[:4] + Rework_Operations[:1] + Rework_Operations[2:4] + Normal_Operations[4:] 
                                
                                # rework + (re-entrance) 12345
                                List11 = Normal_Operations[:2] + Rework_Operations[:2] + Normal_Operations[2:5] + Normal_Operations
                                List22 = Normal_Operations[:3] + Rework_Operations[:3] + Normal_Operations[3:5] + Normal_Operations
                                List33 = Normal_Operations[:3] + Rework_Operations[:1] + Rework_Operations[2:3] + Normal_Operations[3:5] + Normal_Operations
                                List44 = Normal_Operations[:4] + Rework_Operations[:1] + Rework_Operations[2:4] + Normal_Operations[4:5] + Normal_Operations

                                PR_Routes.append(List)
                                PR_Routes.append(List1)
                                PR_Routes.append(List2)
                                PR_Routes.append(List3)
                                PR_Routes.append(List4)
                                PR_Routes.append(List11)
                                PR_Routes.append(List22)
                                PR_Routes.append(List33)
                                PR_Routes.append(List44)

                                File_w.write(" ".join(map(str, List)) + "\n")
                                File_w.write(" ".join(map(str, List1)) + "\n")
                                File_w.write(" ".join(map(str, List2)) + "\n")
                                File_w.write(" ".join(map(str, List3)) + "\n")
                                File_w.write(" ".join(map(str, List4)) + "\n")
                                File_w.write(" ".join(map(str, List11)) + "\n")
                                File_w.write(" ".join(map(str, List22)) + "\n")
                                File_w.write(" ".join(map(str, List33)) + "\n")
                                File_w.write(" ".join(map(str, List44)) + "\n")                         
                                File_w.write("\n")

                                for i in range(Job):
                                    List = []
                                    List1 = []
                                    List2 = []
                                    List3 = []
                                    List4 = []
                                    List11 = []
                                    List22 = []
                                    List33 = []
                                    List44 = []

                                    

                                    List = PT[i] + PT[i]
                                    List1 = PT[i] + PT[i][:2] + [RST[i]] + PT[i][1:2] + PT[i][2:]
                                    List2 = PT[i] + PT[i][:3] + [RST[i]] + PT[i][1:3] + PT[i][3:]
                                    List3 = PT[i] + PT[i][:3] + [RST[i]] + PT[i][2:3] + PT[i][3:]
                                    List4 = PT[i] + PT[i][:4] + [RST[i]] + PT[i][2:4] + PT[i][4:]
                                    List11 = PT[i][:2] + [RST[i]] + PT[i][1:2] + PT[i][2:] + PT[i]
                                    List22 = PT[i][:3] + [RST[i]] + PT[i][1:3] + PT[i][3:] + PT[i]
                                    List33 = PT[i][:3] + [RST[i]] + PT[i][2:3] + PT[i][3:] + PT[i]
                                    List44 = PT[i][:4] + [RST[i]] + PT[i][2:4] + PT[i][4:] + PT[i]

                                    File_w.write(" ".join(map(str, List)) + "\n")
                                    File_w.write(" ".join(map(str, List1)) + "\n")
                                    File_w.write(" ".join(map(str, List2)) + "\n")
                                    File_w.write(" ".join(map(str, List3)) + "\n")
                                    File_w.write(" ".join(map(str, List4)) + "\n")
                                    File_w.write(" ".join(map(str, List11)) + "\n")
                                    File_w.write(" ".join(map(str, List22)) + "\n")
                                    File_w.write(" ".join(map(str, List33)) + "\n")
                                    File_w.write(" ".join(map(str, List44)) + "\n")
                                    File_w.write("\n")
