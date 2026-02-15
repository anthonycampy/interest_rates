# Daily CSV Data Setup

## Install Git

1. Download Git from [git-scm.com/downloads](https://git-scm.com/downloads)
2. Run the installer — the default settings are fine, just click **Next** through each step
3. When finished, search for **Git Bash** in the Start menu to confirm it installed

## One-Time Setup

Open any terminal (Command Prompt, PowerShell, or Git Bash) and run:

```
cd E:\MC\FractalDimension\Strategies
git clone https://github.com/anthonycampy/interest_rates.git
copy interest_rates\data\*.csv .
```

This downloads the repo and copies the CSV files into `E:\MC\FractalDimension\Strategies\`:

```
dgs1.csv
dgs2.csv
dgs5.csv
dgs10.csv
dgs30.csv
t10y2y.csv
dff.csv
```

## Daily Update

Data refreshes at ~4:30 PM ET on weekdays. To pull the latest, open any terminal and run:

```
cd E:\MC\FractalDimension\Strategies\interest_rates && git pull && copy data\*.csv ..
```

## Automate It

1. Create a file called `update-data.bat` on your Desktop with this content:

```bat
cd E:\MC\FractalDimension\Strategies\interest_rates
git pull
copy data\*.csv ..
```

2. Open **Task Scheduler** (search for it in the Start menu)
3. Click **Create Basic Task**
4. Name: `Update Trading Data`
5. Trigger: **Daily**, set time to 5:00 PM, check **weekdays only**
6. Action: **Start a program**, browse to `update-data.bat` on your Desktop
7. Click **Finish**

**Note:** The scheduled task only runs when your computer is on. If it's asleep or off at 5:00 PM, the job is skipped — but that's fine. The next time it runs (or you run `git pull` manually), it will pull all the latest data regardless.
