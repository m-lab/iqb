from iqb import IQB

IQB1 = IQB(name='test1')

print(IQB1.name)

# IQB1.print_config() ### prints detailed info

score1 = IQB1.calculate_iqb_score() 
# score1 = IQB1.calculate_iqb_score(print_details=True) ### print details in the calculation of the iqb score
print(f'IQB score: {score1}')
print()

# IQB1.calculate_iqb_score(data = {}) ### raises an exception



IQB2 = IQB(config='non_existing_file.json') ### raises an exception