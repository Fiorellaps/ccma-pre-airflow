import yaml


class MyDumper(yaml.Dumper):

    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)

def quoted_presenter(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')

yaml.add_representer(str, quoted_presenter)

    
def create_yaml (template_path: str, 
                dets_path:str, 
                application_name:str, 
                code_type:str, 
                image:str, 
                main_application_file:str, 
                main_class:str, 
                arguments:str, 
                driver_cores=2, 
                driver_memory="6144m",
                executor_cores=1, 
                executor_memory="2049m",
                executor_instances=1 ):
    with open(template_path) as file:
        if type(arguments) != list:
            arguments = [arguments]
        list_doc = yaml.safe_load(file)
        list_doc["spec"]["arguments"] = arguments
        list_doc["spec"]["image"] = image
        list_doc["spec"]["type"] = code_type
        list_doc["spec"]["mainApplicationFile"] = main_application_file
        list_doc["spec"]["mainClass"] = main_class
        list_doc["metadata"]["name"] = application_name
        list_doc["spec"]["executor"]["cores"] = executor_cores
        list_doc["spec"]["executor"]["instances"] = executor_instances
        list_doc["spec"]["executor"]["memory"] = executor_memory
        list_doc["spec"]["driver"]["cores"] = driver_cores
        list_doc["spec"]["driver"]["memory"] = driver_memory

    with open(dets_path, "w") as file:
        yaml.dump(list_doc, file, Dumper=MyDumper)
    
    print("yaml written in: " + dets_path)
