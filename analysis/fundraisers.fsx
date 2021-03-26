(*** hide ***)
#load "packages/FsLab/Themes/DefaultWhite.fsx"
#load "packages/FsLab/FsLab.fsx"
type FormattedChart = FormattedChart of XPlot.GoogleCharts.GoogleChart
#if HAS_FSI_ADDHTMLPRINTER
fsi.HtmlPrinterParameters.["html-standalone-output"] <- true
fsi.HtmlPrinterParameters.["grid-row-counts"] <- "100,100"
fsi.HtmlPrinterParameters.["grid-column-counts"] <- "100,100"
fsi.AddHtmlPrinter(fun (FormattedChart ch) ->
  [] :> seq<_>, ch.GetInlineHtml())
#endif
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
      Removed = try r.TryGetAs "Removed" |> OptionalValue.asOption with _ -> None
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
|> Array.filter (fun d -> d.Created.Year > 2020 || (d.Created.Year = 2020 && d.Created.Month > 2))
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
let lastScrape = DateTime.Parse "2021-03-21"

let weeks n = TimeSpan.FromDays(7.0 * float n)
let day n = TimeSpan.FromDays(float n)

let byAge lo hi d = 
  if hi = None then
    // Has been around for at least 'lo' days
    if d.Removed = None then (lastScrape - d.Created).TotalDays >= float lo
    else (d.Removed.Value - d.Created).TotalDays >= float lo
  else 
    // Has been around between 'lo' and 'hi' days
    if d.Removed = None then false
    else 
      (d.Removed.Value - d.Created).TotalDays >= float lo && 
      (d.Removed.Value - d.Created).TotalDays <= float hi.Value

let keys data = 
  [ for d in data -> d.Week ] |> Seq.distinct |> Seq.sort

let realign keys data = 
  let lookup = dict data 
  [ for k in keys -> k, if lookup.ContainsKey k then lookup.[k] else 0 ]

let countsByAge lo hi data = 
  data 
  |> Array.filter (byAge lo hi)
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
  d.Created.Year > 2020 || (d.Created.Year = 2020 && d.Created.Month > 2))

[ countsByAge 0 (Some 28) recent 
  countsByAge 29 (Some 70) recent 
  countsByAge 71 None recent ]
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-10 weeks"; ">10 weeks"]
(*** include-it:f2 ***)
(**
There are several weeks for which we have full data on all three kinds of fundraisers.
These are roughly weeks from 1 week before we started scraping (because all fundraisers
stay at least for a week) until 10 weeks before our last scrape (so that we can identify
kinds of fundraisers started at this point). So, we have full data for this range:
*)
(*** define-output:f3 ***)
let valid = merged |> Array.filter (fun d -> 
  d.Created >= firstScrape - weeks 1 &&
  d.Created < lastScrape - weeks 10 - day 1 )

[ countsByAge 0 (Some 28) valid
  countsByAge 29 (Some 70) valid
  countsByAge 71 None valid ]
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-10 weeks"; ">10 weeks"]
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
    (countsByAge 29 (Some 70) valid)
    (countsByAge 71 None valid)
  |> Seq.map (fun ((sd, s), (ld, l), (vd, v)) ->
    if sd <> ld || ld <> vd then failwith "Should not happen"
    let tot = float (s + l + v)
    sd, (float s / tot, float l / tot, float v / tot) )

[ ratios |> Seq.map (fun (k, (s, l, v)) -> k, s)
  ratios |> Seq.map (fun (k, (s, l, v)) -> k, l)
  ratios |> Seq.map (fun (k, (s, l, v)) -> k, v) ]
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-10 weeks"; ">10 weeks"]
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
  "Variance" => (ratiosFrame |> Stats.variance)
  "Sdv" => (ratiosFrame |> Stats.stdDev) ]
(*** include-it:f5 ***)
(**
## Estimating fundraisers before March
Finally, we're going to calculate the average ratios and estimate the number of fundraisers.
For this, we need some more helper functions. We can now reasonably give the number of 
fundraisers from 8 weeks before our first scrape until 8 weeks before our last scrape 
(we could add undetermined fundraisers at the end, but I'm too lazy to do that...).

For predicted values, the following code also computes a predicted value as multipled by
the estimated ratio - standard deviation and the estimated ratio + standard deviation. This
gives us a range within which we expect the actual values - even if our estimation mechanism
is not perfect.
*)
let getRatios (ratiosFrame:Frame<_, _>) = 
  let m = Stats.mean ratiosFrame
  let s = Stats.stdDev ratiosFrame
  (m?Short, s?Short), (m?Normal, s?Normal), (m?Long, s?Long)

let srat, nrat, lrat = getRatios ratiosFrame

let estimatable = merged |> Array.filter (fun d -> 
  d.Created >= firstScrape - weeks 10 && 
  d.Created < lastScrape - weeks 10 - day 1 )

let tooNew = merged |> Array.filter (fun d ->
  d.Created >= lastScrape - weeks 10)

let estimated f ratio =
  [ for k, v in countsByAge 71 None estimatable ->
      // Calculate predicted, predicted-sdv, predicted+sdv
      let vl = int (float v / fst lrat * fst ratio)
      let vminus = int (float v / fst lrat * (fst ratio - snd ratio))
      let vplus = int (float v / fst lrat * (fst ratio + snd ratio))
      k, f (vl, (vminus, vplus)) ]

let zeroBefore z dt data = 
  [ for d, v in data -> d, if d < dt then z else v ]
let zeroAfter z dt data = 
  [ for d, v in data -> d, if d >= dt then z else v ]
(**
The following is the final chart! It shows the total number of fundraisers per week
(starting on Sunday) for all dates for which we know or can reasonably estimate. It shows
actual data from our first parse for all fundraisers. For regular fundraisers, we estimate
numbers from 8-4 weeks before our first scrape. For short fundraisers, we estimate numbers
from 8-0 weeks before our first scrape.
*)
(*** define-output:f6 ***)
let lines = 
  [ countsByAge 0 (Some 28) estimatable |> zeroBefore 0 firstScrape
    estimated fst srat |> zeroAfter 0 firstScrape
    countsByAge 29 (Some 70) estimatable |> zeroBefore 0 (firstScrape - weeks 4)
    estimated fst nrat |> zeroAfter 0 (firstScrape - weeks 4)
    countsByAge 71 None estimatable ]
let labels = 
  ["0-4 weeks"; "0-4 (pred)"; "4-10 weeks"; "4-10 (pred)"; ">10 weeks"]

lines
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels labels
(*** include-it:f6 ***)
(**
We can add all the data together and show this as a line chart, showing the total number
of fundraisers started. This is based on the mean predictions. The table below lets us
see the error ranges.
*)
(*** define-output:f7 ***)
lines
|> Seq.reduce (List.map2 (fun (d, a) (_, b) -> d, a + b))
|> Chart.Area
(*** include-it:f7 ***)
(**

The full data for each week in a convenient data table that is easy to copy and analyse further.
For each predicted value, we include the mean column, a column based on ratio-sdv and a column
based on a ratio+sdv. The Total column is just the total; Total-sdv sums the low estimates and
Total+sdv sums the high estimates.
*)
(*** define-output:f8 ***)
let trip (k, v) = k, (v, v, v)
let flat (v, (v1, v2)) = v, v1, v2
let linesErr = 
  [ countsByAge 0 (Some 28) estimatable 
      |> Seq.map trip |> zeroBefore (0, 0, 0) firstScrape
    estimated flat srat |> zeroAfter (0, 0, 0) firstScrape
    countsByAge 29 (Some 70) estimatable 
      |> Seq.map trip |> zeroBefore (0, 0, 0) (firstScrape - weeks 4)
    estimated flat nrat |> zeroAfter (0, 0, 0) (firstScrape - weeks 4)
    countsByAge 71 None estimatable |> List.map trip ]

let df1 = 
  frame
    [ for lb, l in Seq.zip labels linesErr do
        yield lb => series l ]
  |> Frame.mapRowKeys (fun rk -> rk.ToString("yyyy-MM-dd"))
  |> Frame.expandAllCols 1

let unks = 
  tooNew 
  |> Array.countBy (fun d -> d.Week) |> series 
  |> Series.mapKeys (fun rk -> rk.ToString("yyyy-MM-dd"))
let df2a = 
  frame [ "unknown" => unks ]

let df2b = 
  Seq.fold (fun df c -> Frame.dropCol c df) df1
    [ for s in ["0-4 weeks"; "4-10 weeks"; ">10 weeks"] do
        yield! [s + ".Item2"; s + ".Item3" ] ]
  |> Frame.mapColKeys (fun ck ->
      ck.Replace("(pred).Item2", "(pred-sdv)")
        .Replace("(pred).Item3", "(pred+sdv)").Replace(".Item1", ""))

let df2 = df2b.Join(df2a) |> Frame.fillMissingWith 0 |> Frame.sortRowsByKey

let tt = df2?``0-4 weeks`` + df2?``4-10 weeks`` + df2?``>10 weeks`` + df2?unknown
let ta = tt + df2?``0-4 (pred)`` + df2?``4-10 (pred)``
let tl = tt + df2?``0-4 (pred-sdv)`` + df2?``4-10 (pred-sdv)``
let th = tt + df2?``0-4 (pred+sdv)`` + df2?``4-10 (pred+sdv)``

let df3 = 
  df2 
  |> Frame.addCol "Total" (Series.mapValues int ta)
  |> Frame.addCol "Total (-sdv)" (Series.mapValues int tl)
  |> Frame.addCol "Total (+sdv)" (Series.mapValues int th)

df3
(*** include-it:f8 ***) 
(**

Now that we have the error ranges, we can also draw a chart showing our estimate alongside
with our estimated error ranges. This suggests that our prediction based on ratio of 
short-term/mid-term/long-term fundraisers is a fairly reasonable - this is quite likely 
less of a source for error than the variance inherent in the data we scraped.
*)
(*** define-output:f9 ***)
[ df3?``Total (-sdv)`` |> Series.mapKeys DateTime.Parse 
  df3?``Total (+sdv)`` |> Series.mapKeys DateTime.Parse
  df3?Total |> Series.mapKeys DateTime.Parse ]
|> Chart.Line
|> Chart.WithOptions(Options(curveType="function", 
    colors=[|"#c0c0c0"; "#c0c0c0"; "#ff7f0e"|]))
|> Chart.WithSize (800, 400)
|> FormattedChart
(*** include-it:f9 ***) 
(**
# Exploring individual donation data

We can perform exactly the same calculation as above to get a chart showing the individual
donations. There is a caveat - we do not have full data on all fundraisers, so this is not
going to give us the correct total number of money donated!

However, as the [earlier analaysis suggests](analysis.html), the fundraisers for which data
is missing do not seem to be special in any way, so we can still reasonably try to do this,
at least to look at the trends.

## Donations per week in our data

As before, let's first look at the individual donation data that we have in our data set.
Note that this is misleading, because we only started scraping data in May - so the donations
for March and April are only those for long-lived fundraisers.
*)
(*** define-output:d1 ***) 
merged 
|> Seq.collect (fun f -> f.Donations)
|> Seq.filter (fun (d, _) -> d.Year > 2020 || (d.Year = 2020 && d.Month > 2))
|> Seq.groupBy (fun (d, _) -> previousSunday d)
|> Seq.map (fun (w, vs) -> w, Seq.sumBy snd vs)
|> Chart.Column
(*** include-it:d1 ***) 

(**
## Donations by fundraiser length

As before, we can show the data based on the kind of fundraiser. For fundraisers that stay
on the web site for >10 weeks, we have data going back to March. For fundraisers that stay online
for shorter period, we only have data for later, but we can estimate those.
*)
(*** define-output:d2 ***) 
let donationsByAge keys lo hi lod hid source = 
  source
  |> Seq.filter (byAge lo hi)
  |> Seq.collect (fun f -> f.Donations)
  |> Seq.filter (fun (d, _) -> d > lod && d < hid)
  |> Seq.groupBy (fun (d, _) -> previousSunday d)
  |> Seq.map (fun (w, vs) -> w, Seq.sumBy snd vs)
  |> realign keys

let tooNewDons = 
  recent
  |> Seq.collect (fun f -> f.Donations)
  |> Seq.filter (fun (d, _) -> d > lastScrape - weeks 10)
  |> Seq.groupBy (fun (d, _) -> previousSunday d)
  |> Seq.map (fun (w, vs) -> w, Seq.sumBy snd vs)

let recentKeys = 
  recent |> Seq.collect (fun d -> d.Donations) 
    |> Seq.map (fst >> previousSunday) |> Seq.distinct |> Seq.sort  

[ recent |> donationsByAge recentKeys 0 (Some 28) firstScrape lastScrape
  recent |> donationsByAge recentKeys 29 (Some 70) (firstScrape - weeks 4) (lastScrape - weeks 4)
  recent |> donationsByAge recentKeys 71 None (firstScrape - weeks 10) (lastScrape - weeks 10) ] 
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-10 weeks"; ">10 weeks"]
(*** include-it:d2 ***) 
(**
Looking at the range for which we have correct data for all fundraiser types, we get:
*)
(*** define-output:d3 ***) 
let validKeys = 
  valid |> Seq.collect (fun d -> d.Donations) 
    |> Seq.map (fst >> previousSunday) |> Seq.distinct 
    |> Seq.filter (fun v -> v < lastScrape - weeks 10) |> Seq.sort  

[ valid |> donationsByAge validKeys 0 (Some 28) (firstScrape - weeks 1) (lastScrape - weeks 10)
  valid |> donationsByAge validKeys 29 (Some 70) (firstScrape - weeks 4) (lastScrape - weeks 10)
  valid |> donationsByAge validKeys 71 None (firstScrape - weeks 10) (lastScrape - weeks 10) ] 
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-10 weeks"; ">10 weeks"]
(*** include-it:d3 ***) 
(**

## Calculating ratios of donations by fundraiser types
As before, we can estimate the ratio of donations belonging to different fundraiser types:

*)
(*** define-output:d4 ***)
let ratiosDon = 
  Seq.zip3
    (valid |> donationsByAge validKeys 0 (Some 28) (firstScrape - weeks 1) (lastScrape - weeks 10))
    (valid |> donationsByAge validKeys 29 (Some 70) (firstScrape - weeks 4) (lastScrape - weeks 10))
    (valid |> donationsByAge validKeys 71 None (firstScrape - weeks 10) (lastScrape - weeks 10))
  |> Seq.map (fun ((sd, s), (ld, l), (vd, v)) ->
    if sd <> ld || ld <> vd then failwith "Should not happen"
    let tot = float (s + l + v)
    sd, (float s / tot, float l / tot, float v / tot) )

[ ratiosDon |> Seq.map (fun (k, (s, l, v)) -> k, s)
  ratiosDon |> Seq.map (fun (k, (s, l, v)) -> k, l)
  ratiosDon |> Seq.map (fun (k, (s, l, v)) -> k, v) ]
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["0-4 weeks"; "4-10 weeks"; ">10 weeks"]
(*** include-it:d4 ***)
(**
There seems to be a bit more variance, but let's proceed and calculate the mean
and standard deviation of the ratios:
*)
(*** define-output:d5 ***)
let ratiosDonFrame = 
  ratiosDon 
  |> Frame.ofRecords
  |> Frame.expandCols ["Item2"]
  |> Frame.indexColsWith ["Week"; "Short"; "Normal"; "Long"]

frame [
  "Mean" => (ratiosDonFrame |> Stats.mean)
  "Variance" => (ratiosDonFrame |> Stats.variance)
  "Sdv" => (ratiosDonFrame |> Stats.stdDev) ]
(*** include-it:d5 ***)
(**
Using the same method as before, we can now draw a chart that combines the actual data for
donations to short term fund-raisers (less than 10 weeks) with estimated data based on 
long-term fundraisers.
*) 
(*** define-output:d6 ***)
let sratd, nratd, lratd = getRatios ratiosDonFrame

let estimatableKeys = 
  merged |> Seq.collect (fun d -> d.Donations) 
    |> Seq.map (fst >> previousSunday) |> Seq.distinct 
    |> Seq.filter (fun v -> v >= firstScrape - weeks 11 && v < lastScrape - weeks 10) |> Seq.sort  

let longDons = 
  merged |> donationsByAge estimatableKeys 71 None (firstScrape - weeks 11) (lastScrape - weeks 10)

let estimatedDons f ratio =
  [ for k, v in longDons ->
      // Calculate predicted, predicted-sdv, predicted+sdv
      let vl = int (float v / fst lrat * fst ratio)
      let vminus = int (float v / fst lrat * (fst ratio - snd ratio))
      let vplus = int (float v / fst lrat * (fst ratio + snd ratio))
      k, f (vl, (vminus, vplus)) ]

let dlines = 
  [ recent |> donationsByAge estimatableKeys 0 (Some 28) (firstScrape - weeks 1) (lastScrape - weeks 10) |> zeroBefore 0 firstScrape
    estimatedDons fst sratd |> zeroAfter 0 firstScrape
    recent |> donationsByAge estimatableKeys 29 (Some 70) (firstScrape - weeks 4) (lastScrape - weeks 10) |> zeroBefore 0 (firstScrape - weeks 4)
    estimatedDons fst nratd |> zeroAfter 0 (firstScrape - weeks 4)
    recent |> donationsByAge estimatableKeys 71 None (firstScrape - weeks 10) (lastScrape - weeks 10) ] 

dlines
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels labels
(*** include-it:d6 ***)
(**
As before, we can add all the data together and show this as a line chart, showing the total number
of donations per week. This is based on the mean predictions. The table below lets us
see the error ranges.
*)
(*** define-output:d7 ***)
lines
|> Seq.reduce (List.map2 (fun (d, a) (_, b) -> d, a + b))
|> Chart.Area
(*** include-it:d7 ***)
(*** define-output:d8 ***)
let linesDonsErr = 
  [ recent |> donationsByAge estimatableKeys 0 (Some 28) (firstScrape - weeks 1) (lastScrape - weeks 10) 
      |> Seq.map trip |> zeroBefore (0, 0, 0) firstScrape
    estimatedDons flat sratd |> zeroAfter (0, 0, 0) firstScrape
    recent |> donationsByAge estimatableKeys 29 (Some 70) (firstScrape - weeks 4) (lastScrape - weeks 10) 
      |> Seq.map trip |> zeroBefore (0, 0, 0) (firstScrape - weeks 4)
    estimatedDons flat nratd |> zeroAfter (0, 0, 0) (firstScrape - weeks 4)
    recent |> donationsByAge estimatableKeys 71 None (firstScrape - weeks 10) (lastScrape - weeks 10) 
      |> List.map trip ] 

let dfd1 = 
  frame
    [ for lb, l in Seq.zip labels linesDonsErr do
        yield lb => series l ]
  |> Frame.mapRowKeys (fun rk -> rk.ToString("yyyy-MM-dd"))
  |> Frame.expandAllCols 1

let unkds = 
  tooNewDons |> series 
  |> Series.sort |> Series.rev
  |> Series.mapKeys (fun rk -> rk.ToString("yyyy-MM-dd"))

let dfd2a = 
  frame [ "unknown" => unkds ]

let dfd2b = 
  Seq.fold (fun df c -> Frame.dropCol c df) dfd1
    [ for s in ["0-4 weeks"; "4-10 weeks"; ">10 weeks"] do
        yield! [s + ".Item2"; s + ".Item3" ] ]
  |> Frame.mapColKeys (fun ck ->
      ck.Replace("(pred).Item2", "(pred-sdv)")
        .Replace("(pred).Item3", "(pred+sdv)").Replace(".Item1", ""))

let dfd2 = dfd2b.Join(dfd2a) |> Frame.fillMissingWith 0 |> Frame.sortRowsByKey

let ttd = dfd2?``0-4 weeks`` + dfd2?``4-10 weeks`` + dfd2?``>10 weeks`` + dfd2?unknown
let tad = ttd + dfd2?``0-4 (pred)`` + dfd2?``4-10 (pred)``
let tld = ttd + dfd2?``0-4 (pred-sdv)`` + dfd2?``4-10 (pred-sdv)``
let thd = ttd + dfd2?``0-4 (pred+sdv)`` + dfd2?``4-10 (pred+sdv)``

let dfd3 = 
  dfd2 
  |> Frame.addCol "Total" (Series.mapValues int tad)
  |> Frame.addCol "Total (-sdv)" (Series.mapValues int tld)
  |> Frame.addCol "Total (+sdv)" (Series.mapValues int thd)

dfd3
(*** include-it:d8 ***) 
(**
Now that we have the error ranges, we can again draw a chart showing our estimate alongside
with our estimated error ranges. 
*)
(*** define-output:d9 ***)
let dfd3nice = dfd3 |> Frame.skip 1
[ dfd3nice?``Total (-sdv)`` |> Series.mapKeys DateTime.Parse 
  dfd3nice?``Total (+sdv)`` |> Series.mapKeys DateTime.Parse
  dfd3nice?Total |> Series.mapKeys DateTime.Parse ]
|> Chart.Line
|> Chart.WithOptions(Options(curveType="function", 
    colors=[|"#c0c0c0"; "#c0c0c0"; "#1f77b4"|]))
|> Chart.WithSize (800, 400)
|> FormattedChart
(*** include-it:d9 ***) 
(**
# The story in two charts
The following shows (again) the two most important charts - the total number of fundraisers
started and the total amount of money donated (per week). It is again worth noting that the
absolute values are somewhat approximate for the number of fundraisers (because our scraping
is limited) and even more approximate for the amounts (because we can only get this data for
some fundraisers). The trends should, however, be mostly right.

*)
(*** define-output:l1 ***)
[ df3?``Total (-sdv)`` |> Series.mapKeys DateTime.Parse 
  df3?``Total (+sdv)`` |> Series.mapKeys DateTime.Parse
  df3?Total |> Series.mapKeys DateTime.Parse ]
|> Chart.Line
|> Chart.WithOptions(Options(curveType="function", 
    colors=[|"#c0c0c0"; "#c0c0c0"; "#ff7f0e"|],
    hAxis=Axis(format="dd/MM/yyyy") ))
|> Chart.WithSize (800, 300)
|> Chart.WithTitle("Number of fundraisers started (per week)")
|> FormattedChart
(*** define-output:l2 ***)
[ dfd3nice?``Total (-sdv)`` |> Series.mapKeys DateTime.Parse 
  dfd3nice?``Total (+sdv)`` |> Series.mapKeys DateTime.Parse
  dfd3nice?Total |> Series.mapKeys DateTime.Parse ]
|> Chart.Line
|> Chart.WithOptions(Options(curveType="function", 
    colors=[|"#c0c0c0"; "#c0c0c0"; "#1f77b4"|],
    hAxis=Axis(format="dd/MM/yyyy") ))
|> Chart.WithTitle("Total amount donated (per week)")
|> Chart.WithSize (800, 300)
|> FormattedChart
(*** include-it:l1 ***)
(*** include-it:l2 ***) 


let ts = 
  merged
  |> Seq.countBy (fun f -> f.Raised / 100)
  //|> Seq.filter (fun (f, c) -> f <> 0 && f < 100)
  |> Seq.sortBy fst
  |> series

Chart.Column(ts)
|> Chart.WithOptions(Options(hAxis=Axis(title="Amount raised (hundreds of £)"), vAxis=Axis(title="Number of fundraisers")))
|> Chart.WithTitle("Number of fundraisers by amount raised (raising less than £10,000)")

(frame [ "Count" => ts ]).SaveCsv("c:/temp/fb/hundreds.csv",keyNames=["Hundreds"])

let ts' = 
  merged
  |> Seq.countBy (fun f -> f.Raised / 1000)
  //|> Seq.filter (fun (f, c) -> f <> 0 && f < 100)
  |> Seq.sortBy fst
  |> series

Chart.Column(ts')
|> Chart.WithOptions(Options(hAxis=Axis(title="Amount raised (thousands of £)"), vAxis=Axis(title="Number of fundraisers")))
|> Chart.WithTitle("Number of fundraisers by amount raised (raising less than £100,000)")

(frame [ "Count" => ts' ]).SaveCsv("c:/temp/fb/thousands.csv",keyNames=["Thousands"])