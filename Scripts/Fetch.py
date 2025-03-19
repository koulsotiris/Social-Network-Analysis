import time
from atproto import Client
import csv
import os

# Authenticate with Bluesky
def authenticate_bluesky(username, password):
    client = Client()
    client.login(username, password)  # Use app password
    return client

# Fetch all with pagination (helper function)
def fetch_all_with_pagination(client, fetch_function, params, max_results=50):
    """
    Generic function to handle paginated API responses with a max results limit.
    """
    results = []
    cursor = None

    while len(results) < max_results:
        if cursor:
            params['cursor'] = cursor
        response = fetch_function(params)

        if hasattr(response, 'feed'):
            items = response.feed
        elif hasattr(response, 'posts'):
            items = response.posts
        elif hasattr(response, 'followers'):
            items = response.followers
        elif hasattr(response, 'follows'):
            items = response.follows
        elif hasattr(response, 'likes'):
            items = response.likes
        else:
            raise ValueError("Unexpected response structure")

        results.extend(items)
        cursor = getattr(response, 'cursor', None)

        if not cursor or len(results) >= max_results:
            break

        time.sleep(0.5)  # Reduced delay for faster fetching

    return results[:max_results]

# Fetch Recent Post URIs
def fetch_recent_post_uris(client, actor_handle, max_results=1):
    """
    Fetch the most recent post for a given actor and return its URI.
    """
    try:
        print(f"Fetching recent posts for {actor_handle}...")
        posts = fetch_all_with_pagination(
            client,
            client.app.bsky.feed.get_author_feed,
            {'actor': actor_handle},
            max_results=max_results
        )
        post_uris = [post.post.uri for post in posts[:max_results] if hasattr(post, 'post') and hasattr(post.post, 'uri')]
        return post_uris
    except Exception as e:
        print(f"Error fetching recent posts for {actor_handle}: {e}")
        return []

# Fetch Likes for a Post
def fetch_likes_for_post(client, post_uri):
    """
    Fetch the list of users who liked a specific post.
    """
    try:
        print(f"Fetching likes for post: {post_uri}...")
        likers = fetch_all_with_pagination(
            client,
            client.app.bsky.feed.get_likes,
            {'uri': post_uri}
        )
        return [liker.actor.handle for liker in likers]
    except Exception as e:
        print(f"Error fetching likes for post {post_uri}: {e}")
        return []

# Recursive Function to Analyze Likes and Expand Network
def analyze_network_recursive(client, handle, depth, current_depth=1, visited=None, gender_cache=None):
    """
    Recursively analyze likes interaction for a user's network.
    """
    if visited is None:
        visited = set()
    if gender_cache is None:
        gender_cache = {}

    if current_depth > depth or handle in visited:
        return []

    visited.add(handle)
    network = []

    # Fetch followers and followings with a max limit of 500
    followers = fetch_all_with_pagination(
        client,
        client.app.bsky.graph.get_followers,
        {'actor': handle},
        max_results=50
    )
    followings = fetch_all_with_pagination(
        client,
        client.app.bsky.graph.get_follows,
        {'actor': handle},
        max_results=50
    )

    print(f"Depth {current_depth}: Found {len(followers)} followers and {len(followings)} followings for {handle}.")

    # Convert followers and followings to sets for easier checks
    follower_handles = {follower.handle for follower in followers}
    following_handles = {following.handle for following in followings}

    # Fetch recent posts of the user
    post_uris = fetch_recent_post_uris(client, handle, max_results=1)

    # Analyze likes for the user's posts
    for post_uri in post_uris:
        likes = fetch_likes_for_post(client, post_uri)
        for follower in followers:
            # Add follower-to-user edge
            network.append({
                'source': follower.handle,
                'target': handle,
                'relation': 'follower',
                'likes': follower.handle in likes
            })
            # Add user-to-follower edge only if mutual
            if follower.handle in following_handles:
                network.append({
                    'source': handle,
                    'target': follower.handle,
                    'relation': 'following',
                    'likes': handle in likes
                })

        for following in followings:
            # Add user-to-following edge
            network.append({
                'source': handle,
                'target': following.handle,
                'relation': 'following',
                'likes': following.handle in likes
            })
            # Add following-to-user edge only if mutual
            if following.handle in follower_handles:
                network.append({
                    'source': following.handle,
                    'target': handle,
                    'relation': 'follower',
                    'likes': handle in likes
                })

    # Recursively analyze followers and followings
    for user in followers + followings:
        if user.handle not in visited:
            network += analyze_network_recursive(client, user.handle, depth, current_depth + 1, visited, gender_cache)

    return network

# Save Network to CSV
def save_network_to_csv(folder, filename, edges):
    fieldnames = ['source', 'target', 'relation', 'gender', 'likes']
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    with open(filepath, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(edges)
    print(f"Network saved to: {filepath}")

# Step 7: Main Function
def main():
    username = "Bluesky_name"  # Replace with Bluesky username
    password = "Bluesky_app_password"  # Replace with Bluesky app password
    target_handle = "markhamillofficial.bsky.social"  # Replace with the target's handle
    depth = 2  # Depth of the recursive analysis

    # Authenticate
    client = authenticate_bluesky(username, password)

    # Analyze the network recursively
    network = analyze_network_recursive(client, target_handle, depth)

    # Save the network to CSV
    output_folder = "output_folder"  # Specify the output folder
    save_network_to_csv(output_folder, 'extended_network.csv', network)

if __name__ == "__main__":
    main()






