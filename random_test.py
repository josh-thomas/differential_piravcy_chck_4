import random


def randomized_response(value, options):
    response = value
    num = random.randint(0, 2)
    if num == 2:
        num = random.randint(0, 2)
        if num == 2:
            response = random.choice([x for x in options if x != value])
            print("noise added")
    return response


def noisy_response(df):
    response_columns = { 
        "VendorID": [1,2],
        "RateCodeID": [1,2,3,4,5,6],
        "Store_and_fwd_flag": ["Y","N"],
        "Payment_type": [1,2,3,4,5,6]
        }
    for column in response_columns:
        temp1 = df[column]
        temp2 = temp1.apply(randomized_response, args=response_columns[column])
        df[column] = temp2


# Payment_type = [1, 2, 3, 4, 5, 6]
# for i in range(100):
#     k = random.randint(1,6)
#     j = randomized_response(k, Payment_type)
#     print("The initial value is ", k, " and with noise it's ", j)

