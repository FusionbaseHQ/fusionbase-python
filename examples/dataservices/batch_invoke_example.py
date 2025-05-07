"""Example demonstrating batch invocation of services."""

import asyncio
import os
import sys

from fusionbase import Fusionbase


async def main():
    """Run the batch invocation example."""
    print("=== Batch Service Invocation Example ===\n")

    # Initialize client with API key from environment variable
    api_key = os.environ.get("FUSIONBASE_API_KEY")
    if not api_key:
        print("ERROR: FUSIONBASE_API_KEY environment variable not set")
        sys.exit(1)

    # Create Fusionbase client
    client = Fusionbase(api_key=api_key)

    try:

        # Get the Company Web Context service key (replace with actual service key)
        service_key = "4658603456"  # Company Web Context service

        # Fetch service metadata asynchronously
        service = await client.services.afrom_key(service_key)
        metadata = await service.aget_metadata()

        print(f"\nSelected service: {metadata.display_name}")
        print(f"Credit cost: {metadata.cost}")
        print("Required inputs:")

        for param in metadata.service_input_definition:
            if param.required:
                sample = param.sample.value if param.sample else "N/A"
                print(f"  {param.name} ({param.type}) - Sample: {sample}")

        # Define example batch inputs for multiple companies
        batch_inputs = [{
            "entity_name": "OroraTech GmbH",
            "postal_code": "81669",
            "street": "St.-Martin-Straße 112",
            "city": "München"
        }, {
            "entity_name": "Microsoft Deutschland GmbH",
            "postal_code": "80992",
            "street": "Walter-Gropius-Straße 5",
            "city": "München"
        }]

        # Check if user wants to proceed (as this will use credits)
        print("\nThis example will use real credits for batch processing.")
        proceed = input("Do you want to proceed? (y/n): ").strip().lower()

        if proceed == 'y':
            print("\nExecuting batch service invoke...")

            # Execute batch invoke asynchronously
            batch_results = await client.services.abatch_invoke_parallel(
                service_key, batch_inputs)

            # Display summarized results
            print(f"\nReceived {len(batch_results)} results:")

            for i, result in enumerate(batch_results):
                company = batch_inputs[i]["entity_name"]
                print(f"\n{i+1}. {company}:")
                print(f"  Result: {result}")
        else:
            print("Batch invocation cancelled.")

            # Show how to use direct invocation with kwargs instead
            print("\nYou can also invoke services with kwargs:")
            print(
                "service.invoke(entity_name='OroraTech GmbH', postal_code='81669', street='St.-Martin-Straße 112', city='München')"
            )

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Always close the client
        await client.aclose()
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
