(*** hide ***)
#load "packages/FsLab/Themes/DefaultWhite.fsx"
#if HAS_FSI_ADDHTMLPRINTER
fsi.HtmlPrinterParameters.["grid-row-counts"] <- "100,100"
fsi.HtmlPrinterParameters.["grid-column-counts"] <- "100,100"
printfn "Yadda!"
#endif
#load "packages/FsLab/FsLab.fsx"
open System
open System.Collections.Generic
open Deedle
open XPlot.GoogleCharts

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
(**
# Estimating data for March fundraisers

Our first data is from May 17. This contains fundraisers from March, but only those that
stayed active for over 8 weeks. Short-lived fundraisers would have been removed from the
web before we started scraping data. In this analysis, I will estimate the number of
short-term fundraisers (<4 weeks) and regular fundraisers (4-8 weeks) based on the number 
of long-term fundraisers (>8 weeks).

## Getting the data

The analysis will use data from our `merged.csv` file produced by another notebook.
We first do some parsing & loading and boring pre-processing of the data.

*)
type Fundraiser = 
  { Source : string
    Link : string 
    Created : DateTime
    Week : DateTime
    MostRecentDonation : DateTime option
    Donations : (DateTime * int)[]
    Raised : int 
    Complete : bool
    Removed : DateTime option }

let parseDonations (s:string) =
  s.Split([|'/'|], StringSplitOptions.RemoveEmptyEntries) |> Array.map (fun s -> 
    let kv = s.Split(' ')
    DateTime.Parse kv.[0], int kv.[1])

let previousSunday (d:DateTime) = 
  let mutable d = d
  while d.DayOfWeek <> DayOfWeek.Sunday do d <- d.AddDays(-1.)
  d
    
let merged = 
  Frame.ReadCsv("../outputs/merged.csv").Rows.Values 
  |> Seq.choose (fun r ->
    if not (r.TryGet("Created").HasValue) then None else
    { Source = r.GetAs "Source"; Link = r.GetAs "Link"; Created = r.GetAs "Created"
      MostRecentDonation = r.TryGetAs "MostRecentDonation" |> OptionalValue.asOption 
      Raised = r.GetAs "Raised"; Complete = r.GetAs "Complete"; 
      Donations = r.GetAs "Donations" |> parseDonations; 
      Removed = r.TryGetAs "Removed" |> OptionalValue.asOption 
      Week = previousSunday (r.GetAs "Created") } |> Some ) 
  |> Array.ofSeq
(**
## Fundraisers in our dataset

First, let's look at the number of fundraisers started in a given week. We only look
at fundraisers started after February 2020. Note that this data is incomplete, because
we do not have information on short-lived fundraisers before May! We may see a peak
in May, but that is wrong, because we are missing some fundraisers before then. 
Note that this also idenfies weeks by Sunday - this is to align with our scrapes that
are done on Sundays.

*)
(*** define-output:f1 ***)
merged 
|> Array.filter (fun d -> d.Created.Year = 2020 && d.Created.Month > 2)
|> Array.countBy (fun d -> d.Week)
|> Chart.Column
(*** include-it:f1 ***)

(**
## Fundraisers by their length

We will now calculate the number of fundraisers in each week based on their type
(short-lived, regular, long-lived). Note that this will leave out some fundraisers
at the end of our date range, because we cannot tell what type will they be!

This will need some data processing - the key function here is `byAge`, which filters
data and returns only fundraisers that have an age in the specified range.
*)
let firstScrape = DateTime.Parse "2020-05-17"
let lastScrape = DateTime.Parse "2020-08-23"

let weeks n = TimeSpan.FromDays(7.0 * float n)
let day n = TimeSpan.FromDays(float n)

let byAge lo hi data = 
  data
  |> Array.filter (fun d -> 
    if hi = None then
      // Has been around for at least 'lo' days
      if d.Removed = None then (lastScrape - d.Created).TotalDays >= float lo
      else (d.Removed.Value - d.Created).TotalDays >= float lo
    else 
      // Has been around between 'lo' and 'hi' days
      if d.Removed = None then false
      else 
        (d.Removed.Value - d.Created).TotalDays >= float lo && 
        (d.Removed.Value - d.Created).TotalDays <= float hi.Value)

let keys data = 
  [ for d in data -> d.Week ] |> Seq.distinct |> Seq.sort

let realign keys data = 
  let lookup = dict data 
  [ for k in keys -> k, if lookup.ContainsKey k then lookup.[k] else 0 ]

let countsByAge lo hi data = 
  byAge lo hi data
  |> Array.countBy (fun d -> d.Week)
  |> realign (keys data)
(**

Now we can use the above helpers to do some visualizations. First, let's look at the
data, but now based on the type of fundraiser. This is the same data as before (with 
missing data before May). There are some more fundraisers removed from July and later,
because we do not yet know their kind (there has not been enough time to tell).
*)
(*** define-output:f2 ***)
let recent = merged |> Array.filter (fun d -> 
  d.Created.Year = 2020 && d.Created.Month > 2)

[ countsByAge 0 (Some 28) recent 
  countsByAge 29 (Some 56) recent 
  countsByAge 57 None recent ]
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-8 weeks"; ">8 weeks"]
(*** include-it:f2 ***)
(**
There are several weeks for which we have full data on all three kinds of fundraisers.
These are roughly weeks from 1 week before we started scraping (because all fundraisers
stay at least for a week) until 8 weeks before our last scrape (so that we can identify
kinds of fundraisers started at this point). So, we have full data for this range:
*)
(*** define-output:f3 ***)
let valid = merged |> Array.filter (fun d -> 
  d.Created >= firstScrape - weeks 1 &&
  d.Created < lastScrape - weeks 8 - day 1 )

[ countsByAge 0 (Some 28) valid
  countsByAge 29 (Some 56) valid
  countsByAge 57 None valid ]
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-8 weeks"; ">8 weeks"]
(*** include-it:f3 ***)
(**
## Calculating ratios of fundraiser types
Using data for the above range, we can calculate average ratio of different
types of fundraisers. First, let's draw a bar chart of the ratios, so that 
we can see how much variance there is between different weeks.
*)
(*** define-output:f4 ***)
let ratios = 
  Seq.zip3
    (countsByAge 0 (Some 28) valid)
    (countsByAge 29 (Some 56) valid)
    (countsByAge 57 None valid)
  |> Seq.map (fun ((sd, s), (ld, l), (vd, v)) ->
    if sd <> ld || ld <> vd then failwith "Should not happen"
    let tot = float (s + l + v)
    sd, (float s / tot, float l / tot, float v / tot) )

[ ratios |> Seq.map (fun (k, (s, l, v)) -> k, s)
  ratios |> Seq.map (fun (k, (s, l, v)) -> k, l)
  ratios |> Seq.map (fun (k, (s, l, v)) -> k, v) ]
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-8 weeks"; ">8 weeks"]
(*** include-it:f4 ***)
(**
Let's now calculate the mean of ratios, together with variance and standard deviation
We're going to assume that this variance is not too unreasonable (but that may be a 
strong assumption).
*)
(*** define-output:f5 ***)
let ratiosFrame = 
  ratios 
  |> Frame.ofRecords
  |> Frame.expandCols ["Item2"]
  |> Frame.indexColsWith ["Week"; "Short"; "Normal"; "Long"]

frame [
  "Mean" => (ratiosFrame |> Stats.mean)
  "Variance" => (ratiosFrame |> Stats.stdDev)
  "Sdv" => (ratiosFrame |> Stats.stdDev) ]
(*** include-it:f5 ***)
(**
## Estimating fundraisers before March
Finally, we're going to calculate the average ratios and estimate the number of fundraisers.
For this, we need some more helper functions. We can now reasonably give the number of 
fundraisers from 8 weeks before our first scrape until 8 weeks before our last scrape 
(we could add undetermined fundraisers at the end, but I'm too lazy to do that...).
*)
let srat, nrat, lrat = 
  let m = Stats.mean ratiosFrame
  m?Short, m?Normal, m?Long

let estimated ratio data =
  [ for k, v in countsByAge 57 None data ->
      k, int (float v / lrat * ratio) ]

let estimatable = merged |> Array.filter (fun d -> 
  d.Created >= firstScrape - weeks 8 && 
  d.Created < lastScrape - weeks 8 - day 1 )

let zeroBefore dt data = 
  [ for d, v in data -> d, if d < dt then 0 else v ]
let zeroAfter dt data = 
  [ for d, v in data -> d, if d >= dt then 0 else v ]
(**
The following is the final chart! It shows the total number of fundraisers per week
(starting on Sunday) for all dates for which we know or can reasonably estimate. It shows
actual data from our first parse for all fundraisers. For regular fundraisers, we estimate
numbers from 8-4 weeks before our first scrape. For short fundraisers, we estimate numbers
from 8-0 weeks before our first scrape.
*)
(*** define-output:f6 ***)
let lines = 
  [ countsByAge 0 (Some 28) estimatable |> zeroBefore firstScrape
    estimated srat estimatable |> zeroAfter firstScrape
    countsByAge 29 (Some 56) estimatable |> zeroBefore (firstScrape - weeks 4)
    estimated nrat estimatable |> zeroAfter (firstScrape - weeks 4)
    countsByAge 57 None estimatable ]
let labels = 
  ["0-4 weeks"; "0-4 (pred)"; "4-8 weeks"; "4-8 (pred)"; ">8 weeks"]

lines
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels labels
(*** include-it:f6 ***)
(**
We can add all the data together and show this as a line chart, showing the total number
of fundraisers started:
*)
(*** define-output:f7 ***)
lines
|> Seq.reduce (List.map2 (fun (d, a) (_, b) -> d, a + b))
|> Chart.Area
(*** include-it:f7 ***)
(**
The full data for each week in a convenient data table that is easy to copy and analyse further:
*)
(*** define-output:f8 ***)
frame
  [ for lb, l in Seq.zip labels lines do
      yield lb => series l 
    yield "Total" => series (Seq.reduce (List.map2 (fun (d, a) (_, b) -> d, a + b)) lines) ]
|> Frame.mapRowKeys (fun rk -> rk.ToString("yyyy-MM-dd"))
(*** include-it:f8 ***)
