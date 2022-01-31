import branch
import creditcard
import customer
import Benefit1
import Benefit2
import Benefit3
import Benefit4
import Insurance
import PlansAttribute
import Network
import ServiceArea
import VA
import VB
import VD
import VE
import VF

def main(): 
    print('Hello!')
    entry = None
    while entry != '0':
        entry = input('\n1) If you would like to see credit card data please press 1\
                    \n2) If you would like to see Health Insurance data please press 2\
                    \n3) If you would like to visualize or analyze uploaded data please press 3\
                    \n4) Otherwise please press 0, and you will be logged out from the system\
                    \n\nPlease choose one of the following options 1, 2, 3, or 4:\
                    \n-------- ')
        if entry =='1':
            case = input('\n1) If you would like to see branch data please press 1\
                    \n2) If you would like to see credit card data please press 2\
                    \n3) If you would like to see customers data please press 3\
                    \n4) Otherwise please press d, and you will return to the previous menu\
                    \n-------- ')
            if case =='1':
                from Case_Study import branch
            elif case=='2':
                from Case_Study import creditcard
            elif case=='3':
                from Case_Study import customer
            else:
                print('You are returned to main menu')
        elif entry =='2':
            case = input('\nHealth Insurance Marketplace Data Files:\
                           \n\t1) BenefitsCostSharing\
                           \n\t2) Insurance\
                           \n\t3) PlanAttributes\
                           \n\t4) Network\
                           \n\t5) ServiceArea\
                           \n\t6) Otherwise please press g, and you will return to the previous menu\
                           \n\nPlease select anything from the list\
                           \n----------- ')
            if case == '1':
                from Case_Study import Benefit1, Benefit2, Benefit3, Benefit4
            elif case =='2':
                from Case_Study import Insurance
            elif case =='3':
                from Case_study import PlansAttribute   
            elif case =='4':
                from Case_Study import Network            
            elif case =='5':
                from Case_Study import ServiceArea
            else:
                print('You are returned to main menu')            
        elif entry =='3':
            case = input('nChoose on of the following options:\
            \n\t1) Here you can see counts of ServiceAreaName, SourceName, and BusinessYear by state\
                           \n\t2) Here you can see the counts of sources across the country\
                           \n\t3) Invalid option\
                           \n\t4) Here you can see the number of benefit plans in each state\
                           \n\t5) Here you can see quantity of smoking mother\
                           \n\t6) Here you can see highest smoking region\
                           \n\t7) Otherwise please press g, and you will return to the previous menu\
                           \n\nPlease select an option from the list\
                           \n------- ')
            if case=='1':
                from Case_Study import VA
            elif case =='2':
                from Case_Study import VB
            elif case =='4':
                from Case_Study import VD         
            elif case =='5':
                from Case_Study import VE            
            elif case =='6':
                from Case_Studt import VF
            else:
                print('Main Menu')
    print('Closed')
                    
        
            
            
if __name__=='__main__':
    main()
