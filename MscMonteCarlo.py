"""
Msc in Data Science
University of Dundee
MonteCarlo Simulation
Infrastructure Design
by Lucho Farje
"""

__author__ = 'Lucho Farje'

import datetime
import random
import adodbapi
import numpy as np

#Global Constansts
MAXTYPEOFCUSTOMERS = 2

CUSTOMER_RETIRED = 1
CUSTOMER_YOUNG_STUDENT = 2

MORNING = 1
AFTERNOON = 2
NIGHT = 3
TOD = [MORNING, AFTERNOON, NIGHT]

MINTOTPOPULATION = 0
MAXTOTPOPULATION = 2000
MAXPOPULATIONPERMITED = 1500

MINTIMEPERIODS = 10
MAXTIMEPERIODS = 40
MAXTIMEAMONTH = 30

NUMBERCITIES = 3

NUMBERSERVERSXCITY = 5
NUMBERTOTALSERVERS = 15

PENALIZATIONONESIM = 0.1
PENALIZATIONTWOSIM = 0.2

DATACENTERJUMBONET = 216000 #Gbph
NETAVAILABLEXSERVR = 14400 #Gbph
NETRECEIVEDXTOWN   = 72000  #Gbph

PROBYESNO = 0.5

USEBANDWIDTHXHOURFILM = 1 #Gbps

MAXHANDLEFILMSXSERVER = 2

MAXDAILYVIDEOSWATCHED = [1,2,3,4,5]
SIMULTANEOUSLYVIDEOSWATCHED = [0,1,2]

MAXLIBRARYVIDEOSNOW = 1000

def getstochasticNumber(pMin, pMax):
    '''
    Stochastically generates and returns a uniformly distributed even number between pMin and pMax
    '''
    xuni = random.randint(pMin, pMax)
    if xuni % 2 == 0:
        return xuni
    while xuni % 2 != 0:
        xuni = random.randint(pMin, pMax)
    return xuni

def getconnectionDB():
    '''
    Create connection to a local database
    '''
    myhost = r".\SQL2014EXPRESS"
    mydatabase = "MscMonteCarlo"
    myuser = "Scientist"
    mypassword = "LuchoF"
    connStr = """Provider=SQLOLEDB.1; User ID=%s; Password=%s;Initial Catalog=%s;Data Source= %s"""
    myConnStr = connStr % (myuser, mypassword, mydatabase, myhost)
    myConn = adodbapi.connect(myConnStr)
    return myConn

def dopreconfigureSim(pgetRandom):
    '''
    Pre Configure Simulation: Select random values and get values from tables
    '''
    lparametersCalculated = []
    
    lYoungPopulation = pgetRandom(MINTOTPOPULATION,MAXTOTPOPULATION)
    lRetiredPopulation = pgetRandom(MINTOTPOPULATION,MAXTOTPOPULATION)
    lToD = random.choice(TOD)
    
    #print lYoungPopulation
    lparametersCalculated.append(lYoungPopulation)
    #print lRetiredPopulation
    lparametersCalculated.append(lRetiredPopulation)
    #print lToD
    lparametersCalculated.append(lToD)
    
    dbconn = getconnectionDB()
    cursorYoungP = dbconn.cursor()
    cursorYoungP.execute("select ReadinessProbability from dbo.TypeCustomerProbWatch where IdTimeOfDay = ? and IdTypeCustomer = ?", (lToD, CUSTOMER_YOUNG_STUDENT))
    lReadinessYoung = cursorYoungP.fetchone()[0]
    #if lReadinessYoung: print lReadinessYoung
    lparametersCalculated.append(lReadinessYoung)
    
    cursorRetiP = dbconn.cursor()
    cursorRetiP.execute("select ReadinessProbability from dbo.TypeCustomerProbWatch where IdTimeOfDay = ? and IdTypeCustomer = ?", (lToD, CUSTOMER_RETIRED))
    
    lReadinessReti = cursorRetiP.fetchone()[0]
    #if lReadinessReti: print lReadinessReti
    lparametersCalculated.append(lReadinessReti)
    
    lPeriodsOfTime = pgetRandom(MINTIMEPERIODS,MAXTIMEPERIODS)
    #print lPeriodsOfTime
    lparametersCalculated.append(lPeriodsOfTime)
    #print lparametersCalculated
    
    lnumpopxCity = (lYoungPopulation + lRetiredPopulation)/NUMBERCITIES #calculate population per City.
    lparametersCalculated.append(lnumpopxCity)
    
    print "1 Preconfiguration Done..."
    return lparametersCalculated, lPeriodsOfTime

def doconfigureSim(psimParam):
    '''
    Configure Simulation: Update value in currentconfig table and insert values in Historyconfig table
    '''
    dbconn = getconnectionDB()
    cursorGetCurPar = dbconn.cursor()
    cursorGetCurPar.execute("select InitPopSizeYS, InitPopSizeRE, PartOfDay, ReadinessProbYS, ReadinessProbRE, PeriodsTimeRunSim, NumberPopulationxCity from dbo.CurrentConfig")
    row = cursorGetCurPar.fetchone()
    #print row.InitPopSizeYS, row.InitPopSizeRE, row.PartOfDay, float(row.ReadinessProbYS), float(row.ReadinessProbRE), row.PeriodsTimeRunSim, row.NumberPopulationxCity
    
    cursorInsHisPar = dbconn.cursor()
    cursorInsHisPar.execute("insert into HistoryConfig(InitPopSizeYS, InitPopSizeRE, PartOfDay, ReadinessProbYS, ReadinessProbRE, PeriodsTimeRunSim, NumberPopulationxCity) values (?, ?, ?, ?, ?, ?, ?)", (row.InitPopSizeYS, row.InitPopSizeRE, row.PartOfDay, float(row.ReadinessProbYS), float(row.ReadinessProbRE), row.PeriodsTimeRunSim, row.NumberPopulationxCity))
    dbconn.commit()
    
    lnumpopxCity = (psimParam[0] + psimParam[1])/NUMBERCITIES #calculate population per City.
    
    cursorUpdCurPar = dbconn.cursor()
    cursorUpdCurPar.execute("update CurrentConfig set InitPopSizeYS = ?, InitPopSizeRE = ?, PartOfDay = ?, ReadinessProbYS = ?, ReadinessProbRE = ?, PeriodsTimeRunSim = ?, NumberPopulationxCity=?", (psimParam[0], psimParam[1], psimParam[2], psimParam[3], psimParam[4], psimParam[5], lnumpopxCity))
    dbconn.commit()
    
    print "2 Configure and Archive Parameters for Simulation Done..."
    return True

def doinitSim(pInitPopY, pInitPopR):
    '''
    Initialize Simulation: Delete all current records and create records for Young Students and Retired
    '''
    dbconn = getconnectionDB()
    cursorDelCust = dbconn.cursor()
    cursorDelCust.execute("delete from dbo.Customer")
    print cursorDelCust.rowcount, ' customer purged'
    dbconn.commit()
    
    cursorDelSumm = dbconn.cursor()
    cursorDelSumm.execute("delete from dbo.SummarySimulation")
    print cursorDelSumm.rowcount, ' summary simulation purged'
    dbconn.commit()
    
    print "Initializing customer table in process..."
    #Get population young students and retired and insert records using a for loop
    for x in range(pInitPopY):
        lvideosxdayY = random.choice(MAXDAILYVIDEOSWATCHED)
        lsimultaneouslyY = random.choice(SIMULTANEOUSLYVIDEOSWATCHED)
        cursorInsCusY = dbconn.cursor()
        cursorInsCusY.execute("insert into Customer(IdSeqTypeCustomer, IdTypeCustomer, VideosWatchedxDay, VideosWatchedSimultaneously) values (?,?,?,?)", ((x+1), CUSTOMER_YOUNG_STUDENT, lvideosxdayY, lsimultaneouslyY))
    for y in range(pInitPopR):
        lvideosxdayR = random.choice(MAXDAILYVIDEOSWATCHED)
        lsimultaneouslyR = random.choice(SIMULTANEOUSLYVIDEOSWATCHED)
        cursorInsCusR = dbconn.cursor()
        cursorInsCusR.execute("insert into Customer(IdSeqTypeCustomer, IdTypeCustomer, VideosWatchedxDay, VideosWatchedSimultaneously) values (?,?,?,?)", ((y+1), CUSTOMER_RETIRED, lvideosxdayR, lsimultaneouslyR))
    dbconn.commit()
    
    print "3 Initialization to run simulation have been done..."

#for MC simulation
def isWatchingAllVideos(pnumVideosToWatch, ptotLibraryVideos, pProb):
    lprobaccu = []
    for i in range(pnumVideosToWatch):
        watched = 0
        for x in range(ptotLibraryVideos):
            if random.random() <= pProb:
                watched += 1
        lprob = watched/float(pnumVideosToWatch)
        lprobaccu.append(lprob)
    #print lprobaccu
    lavgprob = np.mean(lprobaccu)
    #print lavgprob
    if lavgprob >= PROBYESNO:
        return 1
    else:
        return 0

def calculateBandwidthUsed(pVideosWxDay, pVideosSimul):
    lBandwidthxUser = pVideosWxDay * USEBANDWIDTHXHOURFILM
    if pVideosSimul==0:
        return lBandwidthxUser
    else:
        if pVideosSimul==1:
            return (lBandwidthxUser + (lBandwidthxUser*PENALIZATIONONESIM))
        else:
            return (lBandwidthxUser + (lBandwidthxUser*PENALIZATIONTWOSIM))

#def getpereachcustomeraTuple(pPointer, pTypeC):
#    ldbconn = getconnectionDB()
#    cursorSelCus = ldbconn.cursor()
#    cursorSelCus.execute("select VideosWatchedxDay, VideosWatchedSimultaneously from dbo.Customer where IdSeqTypeCustomer = ? and IdTypeCustomer = ?", (pPointer, pTypeC))
#    lrow = cursorSelCus.fetchone()
#    lnv = lrow.VideosWatchedxDay
#    lvs = lrow.VideosWatchedSimultaneously
#    return lnv, lvs

#def isgoodQuality():
#    dbconn = getconnectionDB()
#    cursorSelTotCust = dbconn.cursor()
#    cursorSelTotCust.execute("")
#    return True

def insertcustomersummaryxPeriod(pPeriod, pTypeCustomer, pPopY, pPopR):
    dbconn = getconnectionDB()
    
    lTotPop = (pPopY + pPopR)
    lPopCity = (pPopY + pPopR)/NUMBERCITIES
    
    cursorSelCustomer = dbconn.cursor()
    
    if pTypeCustomer == CUSTOMER_RETIRED:
        cursorSelCustomer.execute("select sum(TotalBandwidthUsed) from dbo.Customer where idtypecustomer = 1")
    else:
        cursorSelCustomer.execute("select sum(TotalBandwidthUsed) from dbo.Customer where idtypecustomer = 2")
    lTotBandW = cursorSelCustomer.fetchone()[0]
    
    if pTypeCustomer == CUSTOMER_RETIRED:
        cursorSelCustomer.execute("select sum(VideosWatchedxDay) from dbo.Customer where idtypecustomer = 1")
    else:
        cursorSelCustomer.execute("select sum(VideosWatchedxDay) from dbo.Customer where idtypecustomer = 2")
    lTotVideos = cursorSelCustomer.fetchone()[0]
    
    lTotBandWxC = int(round((lTotBandW/NUMBERCITIES),0))
    
    #add condition to calculate quality
    if lTotBandW <= DATACENTERJUMBONET:
        lStreamQ = 1
    else:
        lStreamQ = 0
    
    cursorInsSummary = dbconn.cursor()
    cursorInsSummary.execute("insert into SummarySimulation(IdTimePeriod, IdTypeCustomer, TotalPopulationSimulation, TotalPopulationxCity, TotalBandWidthUsed, TotalBandWidthUsedxCity, TotalVideosWatched, StreamQuality) values(?,?,?,?,?,?,?,?)", (pPeriod, pTypeCustomer, lTotPop, lPopCity, lTotBandW, lTotBandWxC, lTotVideos, lStreamQ))
    dbconn.commit()
    
    return True

def totalcapacityReached():
    
    return False

def dorunCoreSim(pPeriodsTime, pInitPopY, pInitPopR, pPReadinessY, pPReadinessR):
    '''
    Run Simulation: Execute simulation for the all time periods obtained by random
    '''
    dbconn = getconnectionDB()
    for i in range(MAXTYPEOFCUSTOMERS):    #loop x type of customer
        ltypecust = (i+1) #it starts from 0
        for pt in range(pPeriodsTime):     #loop x all periods of time (random)
            lperiod = (pt+1)
            #if lperiod == MAXTIMEAMONTH: break
            if ltypecust == CUSTOMER_RETIRED:
                for x in range(pInitPopR):
                    lpointerR = (x+1)
                    lnvideoswxday = random.choice(MAXDAILYVIDEOSWATCHED)
                    lvideoswsimultaneously = random.choice(SIMULTANEOUSLYVIDEOSWATCHED)
                    lwatchedall = isWatchingAllVideos(lnvideoswxday, MAXLIBRARYVIDEOSNOW, pPReadinessR) #function
                    ltotbandw = calculateBandwidthUsed(lnvideoswxday, lvideoswsimultaneously) #function
                    if totalcapacityReached(): break #update rest to jump off and break
                    cursorUpdCusR = dbconn.cursor()
                    cursorUpdCusR.execute("update Customer set VideosWatchedxDay = ?, VideosWatchedSimultaneously = ?, HaveWatchedAllVideos = ?, TotalBandwidthUsed = ? where IdSeqTypeCustomer = ? and IdTypeCustomer = ?", (lnvideoswxday, lvideoswsimultaneously, lwatchedall, ltotbandw, lpointerR, CUSTOMER_RETIRED))
                    dbconn.commit()
                insertcustomersummaryxPeriod(lperiod, CUSTOMER_RETIRED, pInitPopY, pInitPopR)
            else:
                for y in range(pInitPopY):
                    lpointerY = (y+1)
                    lnvideoswxday = random.choice(MAXDAILYVIDEOSWATCHED)
                    lvideoswsimultaneously = random.choice(SIMULTANEOUSLYVIDEOSWATCHED)
                    lwatchedall = isWatchingAllVideos(lnvideoswxday, MAXLIBRARYVIDEOSNOW, pPReadinessY) #function
                    ltotbandw = calculateBandwidthUsed(lnvideoswxday, lvideoswsimultaneously) #function
                    if totalcapacityReached(): break #update rest to jump off and break
                    cursorUpdCusY = dbconn.cursor()
                    cursorUpdCusY.execute("update Customer set VideosWatchedxDay = ?, VideosWatchedSimultaneously = ?, HaveWatchedAllVideos = ?, TotalBandwidthUsed = ? where IdSeqTypeCustomer = ? and IdTypeCustomer = ?", (lnvideoswxday, lvideoswsimultaneously, lwatchedall, ltotbandw, lpointerY, CUSTOMER_YOUNG_STUDENT))
                    dbconn.commit()
                insertcustomersummaryxPeriod(lperiod, CUSTOMER_YOUNG_STUDENT, pInitPopY, pInitPopR)
            print "Periods of Time " + str(lperiod)
        #end for Periods Time
        print "Type of Customer " + str((i+1))
    #end for Type Customers
    print "4 Simulation running have been done..."

def doprintsummarySim(pInitPopY, pInitPopR, pPopCity):
    '''
    Print Summary: Output summary result of this simulation and archive calculated values
    '''
    
    TotPopulation = (pInitPopY + pInitPopR)
    print "The total population executed for this simulation is " + str(TotPopulation) + " and average per city is " + str(pPopCity)
    
    dbconn = getconnectionDB()
    cursorSelSumJO = dbconn.cursor()
    cursorSelSumJO.execute("select count(*) from dbo.Customer where HaveWatchedAllVideos=0")
    lHaveWatched = cursorSelSumJO.fetchone()[0]
    print "Total number of customers which have jumped off " + str(lHaveWatched)
    
    cursorSelTotVideos = dbconn.cursor()
    cursorSelTotVideos.execute("select sum(TotalVideosWatched) from dbo.SummarySimulation")
    lNumberWatched = cursorSelTotVideos.fetchone()[0]
    print "Total number of videos watched for customers population " + str(lNumberWatched)
    
    cursorSelBandWidth = dbconn.cursor()
    cursorSelBandWidth.execute("select sum(TotalBandWidthUsed) from dbo.SummarySimulation")
    lTotBandWidth = cursorSelBandWidth.fetchone()[0]
    print "Total of bandwidth used for deliver this simulation for all time periods " + str(lTotBandWidth)
    
    if lTotBandWidth > DATACENTERJUMBONET:
        print "The total of bandwidth used in all time periods exceeds the capacity of datacenter..."
    else:
        print "The total of bandwidth used in all time periods does not exceeds the capacity of datacenter..."
    
    cursorSelAvgBandWidth = dbconn.cursor()
    cursorSelAvgBandWidth.execute("select avg(TotalBandWidthUsed) from dbo.SummarySimulation")
    lAvgBandWidth = cursorSelAvgBandWidth.fetchone()[0]
    print "Average of bandwidth used for deliver this simulation for all time periods " + str(lAvgBandWidth)
    
    if lAvgBandWidth > DATACENTERJUMBONET:
        print "The average of bandwidth used in all time periods exceeds the capacity of datacenter..."
    else:
        print "The average of bandwidth used in all time periods does not exceeds the capacity of datacenter..."
    
    cursorSelBWxCity = dbconn.cursor()
    cursorSelBWxCity.execute("select sum(TotalBandWidthUsedxCity) from dbo.SummarySimulation")
    lTotBandWidthCity = cursorSelBWxCity.fetchone()[0]
    print "Total of bandwidth used for deliver this simulation for all cities " + str(lTotBandWidthCity)
    
    if lTotBandWidthCity > (NETAVAILABLEXSERVR*6):
        print "The total of bandwidth used for deliver videos for all cities exceeds the capacity of the servers (+1 memory kit)"
    else:
        print "The total of bandwidth used for deliver videos for all cities does not exceeds the capacity of the servers (+1 memory kit)"

    if lTotBandWidthCity > NETRECEIVEDXTOWN:
        print "The total of bandwidth used for deliver videos for all cities exceeds the network capacity received per city"
    else:
        print "The total of bandwidth used for deliver videos for all cities does not exceeds the network capacity received per city"
            
    cursorSelAvgBWxCity = dbconn.cursor()
    cursorSelAvgBWxCity.execute("select avg(TotalBandWidthUsedxCity) from dbo.SummarySimulation")
    lAvgBandWidthCity = cursorSelAvgBWxCity.fetchone()[0]
    print "Average of bandwidth used for deliver this simulation for all cities " + str(lAvgBandWidthCity)
    
    if lAvgBandWidthCity > (NETAVAILABLEXSERVR*6):
        print "The average of bandwidth used for deliver videos for all cities exceeds the capacity of the servers (+1 memory kit)"
    else:
        print "The average of bandwidth used for deliver videos for all cities does not exceeds the capacity of the servers (+1 memory kit)"

    if lAvgBandWidthCity > NETRECEIVEDXTOWN:
        print "The average of bandwidth used for deliver videos for all cities exceeds the network capacity received per city"
    else:
        print "The average of bandwidth used for deliver videos for all cities does not exceeds the network capacity received per city"
    
    cursorSelQoSBad = dbconn.cursor()
    cursorSelQoSBad.execute("select count(*) from dbo.SummarySimulation where StreamQuality = 0")
    lBad = cursorSelQoSBad.fetchone()[0]
    if lBad >= 1:
        cursorSelQoSGood = dbconn.cursor()
        cursorSelQoSGood.execute("select count(*) from dbo.SummarySimulation where StreamQuality = 1")
        lGood = cursorSelQoSGood.fetchone()[0]
        lratio = (lBad/lGood)
        lfinalP = (100-lratio)
        print "The quality of service was " + str(lfinalP)
    else:
        print "The quality of service was good (100%)"
    
    print "5 Simulation summary have been done..."

def main():
    lgetParam, lgetPeriodsTime = dopreconfigureSim(getstochasticNumber) #get random numbers and get probabilities in table
    if (lgetParam[0]+lgetParam[1]) >= MAXPOPULATIONPERMITED:
        print "Simulation have minimun population permited"
        if doconfigureSim(lgetParam):
            doinitSim(lgetParam[0], lgetParam[1]) #create or instantiate the customers in a database
            dorunCoreSim(lgetPeriodsTime, lgetParam[0], lgetParam[1], lgetParam[3], lgetParam[4]) #MC core simulator
            doprintsummarySim(lgetParam[0], lgetParam[1], lgetParam[6]) #print calculations and update QoS
        else:
            print "Error configuring simulation! Aborting..."
    else:
        print "The population calculated does not exceed minimun permitted! Aborting..."
    
if __name__ == "__main__":
    print "Starting MonteCarlo Simulation at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    main()
    print "MonteCarlo Simulation is Finished at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M")