(*** hide ***)
#load "packages/FsLab/Themes/DefaultWhite.fsx"
#load "packages/FsLab/FsLab.fsx"
type FormattedChart = FormattedChart of XPlot.GoogleCharts.GoogleChart
#if HAS_FSI_ADDHTMLPRINTER
fsi.HtmlPrinterParameters.["html-standalone-output"] <- true
fsi.HtmlPrinterParameters.["grid-row-counts"] <- "10,10"
fsi.HtmlPrinterParameters.["grid-column-counts"] <- "10,10"
fsi.AddHtmlPrinter(fun (FormattedChart ch) ->
  [] :> seq<_>, ch.GetInlineHtml())
#endif
open System
open System.Collections.Generic
open Deedle
open XPlot.GoogleCharts

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
(**
# Looking at locations

The GoFundMe data contain some information about locations. This is generally
not very fine-grained (and we have only limited amount of data), but it is
fairly regular. The locations are plain text such as:

 - Chislehurst, Greater London, United Kingdom
 - London, Greater London, United Kingdom
 - Ivybridge, South West England, United Kingdom
 - Aberdeen, Scotland, United Kingdom

In this document, we look at this data to see if the number of fundraisers
and the donated amounts differ per region. We will not look at specific 
areas like "Chislehurst", but at the more coarse-grained regions like 
"South West England" and "Greater London". That way, we (hopefully) have enough 
data to do some aggregations.

## Getting the data
First, we need to read the raw data for all GoFundMe scrapes. This is similar as 
what we do in [Exploring foodbank fundraisers](analysis.html).
*)
let firstScrape = DateTime.Parse "2020-05-17"

let scrapes = 
  [ "2020-05-17"; "2020-06-01"; "2020-06-14"; "2020-06-29"; "2020-07-12"; "2020-07-26"; 
    "2020-08-09"; "2020-08-23"; "2020-09-06"; "2020-09-20"; "2020-10-04"; "2020-10-18"; 
    "2020-11-01"; "2020-11-15"; "2020-11-29"; "2020-12-13"; "2020-12-27"; "2021-01-10"; 
    "2021-01-24"; "2021-02-07"; "2021-02-21"; "2021-03-07"; "2021-03-21"; "2021-04-04"; 
    "2021-04-18"; "2021-05-02"; "2021-05-16"; "2021-05-30"; "2021-06-13"; "2021-06-27"; 
    "2021-07-11"; "2021-07-25"; "2021-08-08"; "2021-08-22"; "2021-09-05"; "2021-09-20"; 
    "2021-10-03"; "2021-10-17"; "2021-10-31"; "2021-11-14"; "2021-11-28"; "2021-12-12"; 
    "2021-12-26"; "2022-01-09"; "2022-01-23"; "2022-02-06"; "2022-02-20"; "2022-03-06";
    "2022-03-20"; "2022-04-04"; "2022-04-17"; "2022-05-01"; "2022-05-15"; // GAP
    "2022-06-12" ]

let mergeFiles (files:seq<string>) = 
  let res = Dictionary<_, _>()
  for f in files do
    let df = Frame.ReadCsv(f,inferTypes=false)
    for r in df.Rows.Values do
      let url = r.GetAs<string>("Link")
      res.[url] <- r.[["Location"; "Created"; "MostRecentDonation"; "Donations"; "Raised"; "Complete"; "Link"]] 
  Frame.ofRows [ for (KeyValue(k,v)) in res -> k => v ]
  |> Frame.dropSparseRows

let files = 
  [ for d in scrapes do 
    for k in ["foodbank"; "food-bank"; "soup-kitchen"; "homeless"] do
    let fn = sprintf "../outputs/%s/%s_%s.csv" d "gofundme" k
    if IO.File.Exists(fn) then yield fn ]

let df = mergeFiles files
let preview = 
  df |> Frame.dropCol "Donations" |> Frame.indexRowsOrdinally
(*** include-value:preview ***)
(**
We will need to look at individual donations (so that we have more precise dates
for when money was donated, so we need to parse the data extracted from GoFundMe earlier).
The following builds an array of `Fundraiser` values to make further processing simple.
*)
type Fundraiser = 
  { Link : string 
    Created : DateTime
    Week : DateTime
    MostRecentDonation : DateTime option
    Location : string
    Donations : (DateTime * int)[]
    Raised : int 
    Complete : bool
    Region : string
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
  df.Rows.Values 
  |> Seq.choose (fun r ->
    if not (r.TryGet("Created").HasValue) then None else
    { Link = r.GetAs "Link"; Created = r.GetAs "Created"; Location = r.GetAs "Location"; 
      Region = 
        if r.GetAs "Location" = "London, United Kingdom" then "Greater London"
        else r.GetAs<string>("Location").Split(',').[1].Trim()
      MostRecentDonation = 
        if String.IsNullOrWhiteSpace(r.TryGetAs("MostRecentDonation").ValueOrDefault) then None 
        else (r.TryGetAs "MostRecentDonation" |> OptionalValue.asOption) 
      Raised = r.GetAs "Raised"; Complete = r.GetAs "Complete"; 
      Donations = r.GetAs "Donations" |> parseDonations; 
      Removed = try r.TryGetAs "Removed" |> OptionalValue.asOption with _ -> None
      Week = previousSunday (r.GetAs "Created") } |> Some ) 
  |> Array.ofSeq
(**
## Basic analysis

Let's start with some basic analysis. In order to work with "per capita" data, 
we need population of individual regions. The following is copied from Wikipedia.
*)
let population = 
  [ "North East England", 2657909
    "North West England", 7052000
    "Yorkshire and the Humber", 5284000
    "East Midlands", 4804149
    "West Midlands", 5713000
    "East of England", 5847000
    "Greater London", 8961989
    "South East England", 8635000
    "South West England", 5289000
    "Northern Ireland", 1893700
    "Wales", 3063456
    "Scotland", 5463300 ]
let populationLookup = dict population
(**

First, we count the number of fundraisers and total amount raised per region
across our entire dataset. The following shows the data per region:
*)
let statsByReg = 
  merged 
  |> Seq.groupBy (fun r -> r.Region)
  |> Seq.map (fun (r, funds) ->
    let n = funds |> Seq.length |> float
    let raised = funds |> Seq.sumBy (fun f -> f.Raised) |> float
    r, series [
      "Fundraisers", n
      "Raised", raised
      "AvgRaised", floor (raised / n)
    ] )
  |> Frame.ofRows
(*** include-value:statsByReg ***)
(**
To get a more meaningful information about fundraisers in individual regions,
we can divide the numbers by population and get number of fundraisers and amount
donated per capita (here, shown per 1M of inhabitants to get nicer numbers):
*)
let byRegPer1M = 
  floor (statsByReg / series population * 1000000)
(*** include-value:byRegPer1M ***)
(**
The number of fundraisers in each region per 1M of inhabitants as a column chart:
*)
(*** define-output:l1 ***)
Chart.Column(byRegPer1M?Fundraisers)
(*** include-it:l1 ***)
(**
The amount raised in each region per 1M of inhabitants as a column chart:
*)
(*** define-output:l2 ***)
Chart.Column(byRegPer1M?Raised)
(*** include-it:l2 ***)
(**

## Fundraisers over time per region

In the following, we will look at the number of fundraisers started over time in 
different regions. This does not consider actual amount of money donated (we look
at this below) - the number of fundraisers is perhaps more an illustration of ambition
or a need, rather than actual generosity.

To get readable output, we will look at donations in a month (the data is fairly noisy,
so if we looked at donations in a week, there would be random peaks). 
The following calculation gets the data in the right format:
*)
let funds = 
  [ for r in merged -> r.Region, DateTime(r.Created.Year, r.Created.Month, 1) ]

let allFundMonths = 
  [ for _, w in funds do
      if w >= firstScrape then yield w ] |> Seq.distinct |> Seq.sort

let fundsByRegAndWeek = 
  funds
  |> Seq.countBy id
  |> Seq.groupBy (fun ((reg, wk), c) -> reg)
(**
First, let's look at the total number of fundraisers started in each week in 
individual region (as a total number). This will be bigger in bigger regions, 
so it is not particularly representative, but it shows the proportion relative 
to the total amount donated.

Note that we are using only actual donations from our scrapes (not estimates from
the first COVID outbreak) so the chart starts from June 2020.
*)
(*** define-output:l6 ***)
fundsByRegAndWeek
|> Array.ofSeq
|> Array.map (fun (reg, fs) ->
  let vlookup = dict [ for (_, w), v in fs -> w, v ]
  [ for d in allFundMonths ->
      d, if vlookup.ContainsKey d then vlookup.[d] else 0 ])
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels (Seq.map fst fundsByRegAndWeek)
(*** include-it:l6 ***)
(**
Now, let's look number of fundraisers per capita. In the following, we divide the 
number of fundraisers by the number of inhabitants living in the region, so 
the curves should be comparable across multiple regions.
*)
(*** define-output:l7 ***)
fundsByRegAndWeek
|> Array.ofSeq
|> Array.map (fun (reg, fs) ->
  let vlookup = dict [ for (_, w), v in fs -> w, v ]
  [ for d in allFundMonths ->
      d, 
      if vlookup.ContainsKey d then 
        float vlookup.[d] / float populationLookup.[reg] else 0. ])
|> Chart.Line
|> Chart.WithOptions(Options(curveType="function")) 
|> Chart.WithLabels (Seq.map fst fundsByRegAndWeek)
(*** include-it:l7 ***)

(**
## Donations over time per region

In the following, we will look at individual donations over time in different regions.
For this, we'll use data on individual donations - we have those for about half
of the fundraisers. Generally, the "incomplete" fundraisers are those that are active
for longer period of time (because they have more individual donations than our scraping
can fetch). So, we get more accurate dates, but bias towards short-term fundraisers.
*)
(*** define-output:l3 ***)
merged
|> Seq.countBy (fun r -> r.Complete)
|> Seq.map (fun (k, v) -> (if k then "Complete" else "Incomplete"), v)
|> series
(*** include-it:l3 ***)
(**
To get readable output, we will look at donations in a month (the data is fairly noisy,
so if we looked at donations in a week, there would be random peaks). 
The following calculation gets the data in the right format:
*)
let dons = 
  [ for r in merged do
      for d, v in r.Donations do
        yield r.Region, DateTime(d.Year, d.Month, 1), v ]

let allMonths = 
  [ for _, w, _ in dons do
      if w >= firstScrape then yield w ] |> Seq.distinct |> Seq.sort

let donsByRegAndMonth = 
  dons
  |> Seq.groupBy (fun (reg, month, d) -> reg, month)
  |> Seq.map (fun ((reg, month), ds) -> 
      reg, (month, Seq.sumBy (fun (_, _, d) -> d) ds))
  |> Seq.groupBy fst
(**
First, let's look at the total amount donated per month in individual region
(as a total number). This will be bigger in bigger regions, so it is not 
particularly representative, but it shows the proportion relative to the total
amount donated.

Note that we are using only actual donations from our scrapes (not estimates from
the first COVID outbreak) so the chart starts from June 2020.
*)
(*** define-output:l4 ***)
donsByRegAndMonth
|> Array.ofSeq
|> Array.map (fun (reg, ds) ->
  let vlookup = dict [ for _, (d, v) in ds -> d, v ]
  [ for d in allMonths ->
      d, if vlookup.ContainsKey d then vlookup.[d] else 0 ])
|> Chart.Column
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels (Seq.map fst donsByRegAndMonth)
(*** include-it:l4 ***)
(**
Now, let's look donations per capita. In the following, we divide the 
amount donated by the number of inhabitants living in the region, so 
the curves should be comparable across multiple regions.
*)
(*** define-output:l5 ***)
donsByRegAndMonth
|> Array.ofSeq
|> Array.map (fun (reg, ds) ->
  let vlookup = dict [ for _, (d, v) in ds -> d, v ]
  [ for d in allMonths ->
      d, 
      if vlookup.ContainsKey d then 
        float vlookup.[d] / float populationLookup.[reg] else 0. ])
|> Chart.Line
|> Chart.WithOptions(Options(curveType="function")) 
|> Chart.WithLabels (Seq.map fst donsByRegAndMonth)
(*** include-it:l5 ***)