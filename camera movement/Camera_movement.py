from onvif import ONVIFCamera
import time

# Camera connection details
camera_ip = '10.0.0.6'  # Replace with your camera's IP address
camera_port = 8000  # ONVIF port (default is 80, but check your camera's settings)
username = 'admin'  # Replace with your camera username
password = 'parqueoApp'  # Replace with your camera password

# Initialize the ONVIF camera
camera = ONVIFCamera(camera_ip, camera_port, username, password)

# Get the PTZ service
ptz_service = camera.create_ptz_service()
media_service = camera.create_media_service()

# Get the first profile token
profiles = media_service.GetProfiles()
profile_token = profiles[0].token  # Use the first profile
print(f"Using profile token: {profile_token}")

# Function to move the camera continuously
def continuous_move(ptz_service, profile_token, pan_speed, tilt_speed, zoom_speed, duration):
    try:
        request = ptz_service.create_type('ContinuousMove')
        request.ProfileToken = profile_token
        request.Velocity = {
            'PanTilt': {'x': pan_speed, 'y': tilt_speed},  # Pan and Tilt speeds
            'Zoom': {'x': zoom_speed}  # Zoom speed
        }
        ptz_service.ContinuousMove(request)
        time.sleep(duration)  # Move for the specified duration
        stop_camera(ptz_service, profile_token)  # Stop after the duration
    except Exception as e:
        print(f"Error during ContinuousMove: {e}")

# Function to stop the PTZ movement
def stop_camera(ptz_service, profile_token):
    try:
        request = ptz_service.create_type('Stop')
        request.ProfileToken = profile_token
        request.PanTilt = True
        request.Zoom = True
        ptz_service.Stop(request)
    except Exception as e:
        print(f"Error stopping camera: {e}")

# Full ROM logic with 3 stops
try:
    print("Starting PTZ full ROM...")
    

    # Move to the first stop: Pan left
    continuous_move(ptz_service, profile_token, pan_speed=-1, tilt_speed=0.0, zoom_speed=0.0, duration=2)
    print("Reached Stop 1")

    time.sleep(1)  # Move for the specified duration

    # Move to the second stop: Tilt up
    continuous_move(ptz_service, profile_token, pan_speed=-1, tilt_speed=0.0, zoom_speed=0.0, duration=2)
    print("Reached Stop 2")

    time.sleep(1)  # Move for the specified duration

    # Return to the starting position
    continuous_move(ptz_service, profile_token, pan_speed=1, tilt_speed=0, zoom_speed=0.0, duration=4)
    print("Returned to the starting position.")

    print("PTZ full ROM completed successfully!")

except Exception as e:
    print(f"An error occurred: {e}")