import streamlit as st
from src.main import MainProcess
import threading
import time

# Page title and header
st.title("Administrative Agent")
st.header("Semantic Search Generation")

# Button that triggers the process
run_code = st.button("GENERATE RESEARCH")

if run_code:
    st.write("**Execution started...**")

    # A placeholder where we will display our HTML logs
    log_placeholder = st.empty()

    # Instantiate your main process (which captures logs in a StringIO)
    main = MainProcess(streamlit=True)
    log_stream = main._log_stream

    def run_main_process():
        """Runs the main process in a separate thread."""
        try:
            main.run()
        except Exception as e:
            st.error(f"An error occurred: {e}")
            raise

    # Start the main process in a separate thread
    process_thread = threading.Thread(target=run_main_process)
    process_thread.start()

    # Keep track of old logs so we only update when there's something new
    old_logs = ""

    # While the thread is running, poll for new logs
    while process_thread.is_alive():
        new_logs = log_stream.getvalue()
        if new_logs != old_logs:
            old_logs = new_logs

            # Convert the log string into HTML-safe text (just replace \n -> <br>)
            logs_as_html = new_logs.replace("\n", "<br>")

            # Build HTML + JS snippet to auto-scroll
            html_code = f"""
            <div style="height:300px; overflow:auto; font-family:monospace;" id="log-container">
                {logs_as_html}
            </div>
            <script>
            var logContainer = document.getElementById('log-container');
            if (logContainer) {{
                logContainer.scrollTop = logContainer.scrollHeight;
            }}
            </script>
            """

            # Update the placeholder with our auto-scrolling console
            log_placeholder.markdown(html_code, unsafe_allow_html=True)

        time.sleep(0.5)

    # Final update once the thread is done
    final_logs = log_stream.getvalue()
    logs_as_html = final_logs.replace("\n", "<br>")
    final_html_code = f"""
    <div style="height:300px; overflow:auto; font-family:monospace;" id="log-container-final">
        {logs_as_html}
    </div>
    <script>
    var logContainerFinal = document.getElementById('log-container-final');
    if (logContainerFinal) {{
        logContainerFinal.scrollTop = logContainerFinal.scrollHeight;
    }}
    </script>
    """

    # Show final logs
    log_placeholder.markdown(final_html_code, unsafe_allow_html=True)

    # Indicate successful completion
    st.success("Execution completed successfully!")
