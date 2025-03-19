from atproto import Client
import gender_guesser.detector as gender
import pandas as pd

# Load the dataset
nodes_file_path = "filepath" #replace with filepath
nodes_data = pd.read_csv(nodes_file_path)

# Authenticate with Bluesky
def authenticate_bluesky(username, password):
    print("Authenticating with Bluesky...")
    client = Client()
    client.login(username, password)
    print("Authentication successful!")
    return client

# Fetch display names for Bluesky handles
def fetch_display_names(client, handles):
    display_names = []
    for idx, handle in enumerate(handles, 1):
        try:
            profile = client.app.bsky.actor.get_profile({'actor': handle})
            print(f"[{idx}/{len(handles)}] Full profile for {handle}: {profile}")
            
            # Fetch display name or use the handle as fallback
            display_name = getattr(profile, 'display_name', '')
            if not display_name:
                print(f"[{idx}/{len(handles)}] No display name found for {handle}, using handle instead.")
                display_name = handle
            display_names.append(display_name)
            print(f"[{idx}/{len(handles)}] Display name: {display_name}")
        except Exception as e:
            print(f"[{idx}/{len(handles)}] Error fetching profile for {handle}: {e}")
            display_names.append(handle)  # Use handle if profile fetch fails
    return display_names

# Infer gender from display names
def infer_gender(display_names):
    print("Inferring gender from display names...")
    detector = gender.Detector()
    genders = []
    for idx, name in enumerate(display_names, 1):
        if name:
            # Use the first word of the name for gender inference
            gender_result = detector.get_gender(name.split()[0])
        else:
            gender_result = 'unknown'
        genders.append(gender_result)
        print(f"[{idx}/{len(display_names)}] Display name: '{name}' => Gender: {gender_result}")
    return genders

# Main script
def main():
    # Bluesky credentials
    username = "Bluesky_username"  # Replace with Bluesky username
    password = "Bluesky_app_password"  # Replace with Bluesky app password

    # Authenticate
    client = authenticate_bluesky(username, password)

    # Fetch display names
    print("Fetching display names for nodes...")
    nodes_data['Display Name'] = fetch_display_names(client, nodes_data['Id'])

    # Infer gender
    print("Inferring gender for nodes...")
    nodes_data['Gender'] = infer_gender(nodes_data['Display Name'])

    # Save updated dataset
    output_path = "output_path" #replace with output path
    nodes_data.to_csv(output_path, index=False)
    print(f"Updated dataset saved as '{output_path}'")

if __name__ == "__main__":
    main()
