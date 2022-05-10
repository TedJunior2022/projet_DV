from DevEntity import DevEntity


class Hub(DevEntity):

    def __init__(self, name, business_key, fields):
        self.name = name
        self.business_key = business_key
        self.fields = fields

    
    def get_name(self):
        print("getter method called")
        return self.name
       
     # a setter function
 
    def set_name(self, name):
        self.name = name
    

    def get_business_key(self):
        print("getter method called")
        return self.business_key
       
     # a setter function
 
    def set_business_key(self, business_key):
        self.business_key = business_key

    def __str__(self):
        return "I am the " + self.name 




