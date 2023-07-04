from data_processor.stackoverflow_data_processor import StackOverflowDataProcessor

def main():
    # Configuration variables
    tag = 'python'
    from_date = '2023-01-01'
    destination_path = './content/stackoverflow_data'

    # Create an instance of StackOverflowDataProcessor
    processor = StackOverflowDataProcessor()

    # Fetch Stack Overflow data
    data = processor.fetch_stackoverflow_data(tag, from_date)

    if data is not None:
        # Process the data to get the top trending tags
        top_tags = processor.get_top_trending_tags(data, count=10)

        # Write the Stack Overflow data into a lake using Spark
        processor.write_into_lake(data, destination_path)
        print("Data has been successfully written to the lake.")
        return top_tags
    else:
        print("Failed to fetch Stack Overflow data. Please check your network connection or API availability.")

if __name__ == "__main__":
    main()