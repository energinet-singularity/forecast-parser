import logging

log = logging.getLogger()

def setup_mock_data(app_folder, forecast_folder):
    # Run creation of mock-data at first coming x:30 absolute time
    next_run = (dt.now()).replace(microsecond=0, second=15, minute=0)
    if dt.now().minute >= 30:
        next_run += td(hours=1)

    timer.enterabs(
        next_run.timestamp(),
        1,
        generate_dummy_input,
        (app_folder, forecast_folder, timer),
        )

def generate_dummy_input(template_path: str, output_path: str, timer: scheduler = None):
    """Take templates from 'template_path', update timestamps and output them to 'output_path'.

    Parameters
    ----------
    template_path : str
        The path where the templates can be found.
    output_path : str
        The path where the mock data should be written to.
    timer : scheduler
        (Optional) If specified, a new run will be scheduled for now + ~1h
    """

    log.info("Running dummy-input generation")
    if timer is not None:
        next_run = (dt.now()).replace(microsecond=0, second=0, minute=30) + td(hours=1)
        timer.enterabs(
            next_run.timestamp(),
            1,
            generate_dummy_input,
            (template_path, output_path, timer),
        )

    # Make template-file config
    CONFIG_DATA = {
        r"^ENetNEA_\d{10}\.txt$": {
            "filename": "ENetNEA_<timestamp>.txt",
            "delay_h": 3,
            "timelist": [1, 4, 7, 10, 13, 16, 19, 22],
        },
        r"^EnetEcm_\d{10}\.txt$": {
            "filename": "EnetEcm_<timestamp>.txt",
            "delay_h": 7,
            "timelist": [8, 20],
        },
        r"^ConWx_prog_\d{10}_048\.dat$": {
            "filename": "ConWx_prog_<timestamp>_048.dat",
            "delay_h": 5,
            "timelist": [0, 6, 12, 18],
        },
        r"^ConWx_prog_\d{10}_180\.dat$": {
            "filename": "ConWx_prog_<timestamp>_180.dat",
            "delay_h": 7,
            "timelist": [2, 8, 14, 20],
        },
    }

    print("Files and Directories in '% s':" % template_path)
    obj = os.scandir(template_path)
    for entry in obj:
        if entry.is_dir() or entry.is_file():
            print(entry.name)
    obj.close()
    # Go through all files in template path and check if they fit any of the config-regex
    for template_file in [
        fs_item for fs_item in os.scandir(template_path) if fs_item.is_file()
    ]:
        for config in [
            CONFIG_DATA[key]
            for key in CONFIG_DATA.keys()
            if re.search(key, template_file.name)
        ]:
            print("I was here")
            # Determine if the file is expected to arrive 'now'
            if dt.now().hour in config["timelist"]:
                with open(template_file.path) as f:
                    template = f.read().split("\n")
                new_timestamp = time.strftime(
                    r"%Y%m%d%H", (dt.now() - td(hours=config["delay_h"])).timetuple()
                )
                new_filename = config["filename"].replace("<timestamp>", new_timestamp)
                with open(os.path.join(output_path, new_filename), "w") as f:
                    f.write(
                        "\n".join(
                            change_dummy_timestamp(
                                template, dt.now() - td(hours=config["delay_h"])
                            )
                        )
                    )
                log.info(f"Created dummy-file '{new_filename}'")
                continue


def change_dummy_timestamp(
    contents: list[str], new_t0: dt = dt.now(), max_forecast_h: int = 1000
) -> list[str]:
    """Takes file contents (contents) as a list of lines and then changes the timestamp base on new_t0.

    Parameters
    ----------
    contents : list[str]
        Contents of the forecast-template.
    new_t0 : datetime.datetime
        (Optional) The new t0-timestamp.
        Default = now().
    max_forecast_h : int
        (Optional) Maximum hours - only used to somewhat verify regex value is a timestamp.
        Default = 1000 (leave alone if unsure)
    """
    # All templates have the t0 time at the end of first line after a '='-sign
    time_string = contents[0].replace(" ", "").split("=")[-1]
    template_t0 = dt.fromtimestamp(time.mktime(time.strptime(time_string, r"%Y%m%d%H")))

    new_contents = []
    for row in contents:
        new_row = row
        # Regex: Look for exactly 10 digits with no digits right before or after
        for match in re.finditer(r"(?<!\d)(\d{10})(?!\d)", row):
            try:
                template_time = dt.fromtimestamp(
                    time.mktime(time.strptime(match.group(0), r"%Y%m%d%H"))
                )
            except Exception:
                pass
            else:
                if (
                    0
                    <= (template_time - template_t0).total_seconds() / 3600
                    < max_forecast_h
                ):
                    new_time = new_t0 + (template_time - template_t0)
                    new_row = (
                        new_row[: match.start()]
                        + time.strftime(r"%Y%m%d%H", new_time.timetuple())
                        + new_row[match.end() :]
                    )
        new_contents.append(new_row)

    return new_contents