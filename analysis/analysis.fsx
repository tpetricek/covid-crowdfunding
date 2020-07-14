(*** hide ***)
#load "packages/FsLab/Themes/DefaultWhite.fsx"
#load "packages/FsLab/FsLab.fsx"
open System
open System.Collections.Generic
open Deedle
open XPlot.GoogleCharts

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
(**
<h1 style="font-size:34pt">Exploring foodbank fundraisers</h1>

---

# Merge data

Merge data for "foodbank", "food bank" and "soup kitchen" searche.
We iterate over the files from the oldest to the newest. We use 
fundraiser URL as the key and we always replace older data with 
newer. This eliminates duplicates (from same day scrapes) and also
replaces older records with newer (with more data about individual
donations hopefully).

*)
let mergeFiles (files:seq<string * string>) = 
  let res = Dictionary<_, _>()
  for s, f in files do
    let df = Frame.ReadCsv(f,inferTypes=false)
    for r in df.Rows.Values do
      let url = r.GetAs<string>("Link")
      res.[url] <- 
        Series.merge (series ["Source" => box s])
          r.[["Link"; "Created"; "MostRecentDonation"; "Donations"; "Raised"; "Complete"]] 
  Frame.ofRows [ for (KeyValue(k,v)) in res -> k => v ]
  |> Frame.dropSparseRows

let files = 
  [ for s in ["gofundme"; "just"; "virgin"] do
    for d in ["2020-05-17"; "2020-06-01"; "2020-06-14"; "2020-06-29"; "2020-07-12" ] do 
    for k in ["foodbank"; "food-bank"; "soup-kitchen"] do
    yield s, sprintf "../outputs/%s/%s_%s.csv" d s k ]

let merged = mergeFiles files
(**
# Individual donations

We have individual donation data for some of the fundraisers but not 
for all of them. See for how many we have full data on individual 
donations.

## Check missing data
*)
let parseDonations (s:string) =
  s.Split([|'/'|], StringSplitOptions.RemoveEmptyEntries) |> Array.map (fun s -> 
    let kv = s.Split(' ')
    DateTime.Parse kv.[0], int kv.[1])

let bysource = 
  merged 
  |> Frame.pivotTable 
    (fun _ row -> row.GetAs<string> "Source")
    (fun _ row -> 
      if parseDonations (row.GetAs "Donations") |> Seq.sumBy snd = row.GetAs "Raised"
      then "complete" else "incomplete" )
    Frame.countRows

let byraised = 
  merged 
  |> Frame.pivotTable 
    (fun _ row -> 
      let r = row.GetAs<int> "Raised"
      if r < 100 then "less than 100" elif r < 500 then "less than 500"
      elif r < 1000 then "less than 1k" elif r < 10000 then "less than 10k"
      else "over 10k" )
    (fun _ row -> 
      if parseDonations (row.GetAs "Donations") |> Seq.sumBy snd = row.GetAs "Raised"
      then "complete" else "incomplete" )
    Frame.countRows
(**
What kind of data is missing? The following table shows how many records
with incomplete/complete data we have for different data sources:
*)
(*** include-value:bysource ***)
(**
Similarly, the following table shows how many complete/incomplete records
we have based on then total amount of money raised:

*)
(*** include-value:byraised ***)
(**
## Print some basic statistics
For all donations:
*)
let statsall = 
  ["Median", merged?Raised |> Stats.median
   "Mean", merged?Raised |> Stats.mean
   "Sum", merged?Raised |> Stats.sum
   "Count", merged?Raised |> Stats.count |> float ] |> series
(*** include-value:statsall ***)
(**
For donations where we have all donation data:
*)
let mergeddet = 
  merged
  |> Frame.filterRows (fun _ row -> 
      let dons = row.GetAs<string> "Donations" |> parseDonations
      dons.Length <> 0 && dons |> Seq.sumBy snd = row.GetAs "Raised" )

let statsdet = 
  ["Median", mergeddet?Raised |> Stats.median
   "Mean", mergeddet?Raised |> Stats.mean
   "Sum", mergeddet?Raised |> Stats.sum
   "Count", mergeddet?Raised |> Stats.count |> float ] |> series
(*** include-value:statsdet ***)


(**
## Donations over time
Let's see how are donations typically distributed over the time of a fundraiser.
The following calculates amount donated per week. We sort the fundraisers randomly,
so that we can later take represenative first few for charting.

*)
let traces blockLength filter = 
  let rnd = Random(0)
  mergeddet.Rows 
  |> Series.filter (fun _ row -> filter row?Raised)
  |> Series.map (fun _ row ->
      let dons = parseDonations (row.GetAs "Donations")
      let created = row.GetAs<DateTime> "Created"
      let dons = dons |> Array.map (fun (d, v) -> max 0 (int (d - created).TotalDays), v)
      let max = dons |> Seq.map fst |> Seq.max
      let res = Array.zeroCreate ((max + 1) / blockLength + 1)
      for i, d in dons do res.[i/blockLength] <- res.[i/blockLength] + d
      res )
  |> Series.values
  |> Array.ofSeq
  |> Array.sortBy (fun _ -> rnd.NextDouble())
(**
In the following charts, X axis represents weeks and Y axis the 
amount of money donated in a given week.

### Bigger fundraisers
For bigger donations (raised more than the median), there are some 
fundraisers that run for a very long time (presumably with no end)

*)
(*** define-output:tc1 ***)
Chart.Line 
  [ for t in Seq.truncate 100 (traces 7 (fun d -> d > 225.)) -> 
    Seq.truncate 40 (Seq.indexed t) ]
(*** include-it:tc1 ***)
(**
Most of the activity typically happens in the first 5 weeks,
but some have a second peak just before the end (e.g. some event):
*)
(*** define-output:tc1b ***)
Chart.Line 
  [ for t in Seq.truncate 100 (traces 7 (fun d -> d > 225.)) -> 
    Seq.truncate 20 (Seq.indexed t) ]
(*** include-it:tc1b ***)
(**
### Small fundraisers
For smaller donations (raised less than the median), there are 
very rarely fundraisers that raise some money after 5 weeks:

*)
(*** define-output:tc2 ***)
Chart.Line 
  [ for t in Seq.truncate 100 (traces 7 (fun d -> d < 225.)) -> 
    Seq.truncate 20 (Seq.indexed t) ]
(*** include-it:tc2 ***)
(**
Most of the activity typically happens in the first 2 or 3 weeks:
*)
(*** define-output:tc2b ***)
Chart.Line 
  [ for t in Seq.truncate 100 (traces 7 (fun d -> d < 225.)) -> 
    Seq.truncate 5 (Seq.indexed t) ]
(*** include-it:tc2b ***)
(**
### Total amount donated
The following shows the total amount donated per week of a fundraiser:
*)
(*** define-output:tca ***)
traces 7 (fun d -> true)
|> Seq.reduce (fun a1 a2 -> Array.init (max a1.Length a2.Length) (fun i ->
    (if i < a1.Length then a1.[i] else 0) + (if i < a2.Length then a2.[i] else 0) ))
|> Seq.truncate 30
|> Chart.Line
(*** include-it:tca ***)
(**

# Covid donations
Now, let's see if there is more activity during the coronavirus crisis.

For now, we're only going to use fundraisers for which we have complete data.
This covers about 1/3 of all the fundraisers and the average amount rised is
slightly smaller for this group (we look at somewhat smaller fundraisers), 
but there is enough of them so we can believe it's representative.

Eventually, it would be nice to use all fundraisers, but for that, we'll need
to do some curve fitting and estimate how the donations are distributed for
the fundraisers where we do not have details...

*)
type Clean = 
  { Created:DateTime; Raised:int; Donations:(DateTime*int)[] }

let clean =
  merged.Rows.Values
  |> Seq.choose (fun row -> 
      let dons = row.GetAs<string> "Donations" |> parseDonations
      if dons.Length = 0 || dons |> Seq.sumBy snd <> row.GetAs "Raised" then None
      else Some { Created=row.GetAs<DateTime> "Created"; 
        Raised=int row?Raised; Donations=dons } )
  |> Array.ofSeq

let previousMonday (d:DateTime) = 
  let mutable d = d
  while d.DayOfWeek <> DayOfWeek.Monday do d <- d.AddDays(-1.)
  d
(**

## Fundraisers started per week

The following chart shows the number of fundraisers that  were started during 
individual weeks of 2020. Each bar corresponds to one week (the date of the bar 
is always a Monday and the number is total number of fundraisers in the following
week).
*)
(*** define-output:cw1 ***)
clean
|> Array.filter (fun d -> d.Created.Year = 2020)
|> Array.countBy (fun d -> previousMonday d.Created)
|> Array.sortBy fst
|> Chart.Column
(*** include-it:cw1 ***)
(**

## Donations made per week

The following chart shows the total amounts donated during individual weeks of 
2020 in the same way as the previous chart:
*)
(*** define-output:cw2 ***)
clean
|> Array.collect (fun d -> d.Donations)
|> Array.filter (fun (d, _) -> d.Year = 2020)
|> Array.groupBy (fun (d, _) -> previousMonday d)
|> Array.map (fun (d, g) -> d, Seq.sumBy snd g)
|> Array.sortBy fst
|> Chart.Column
(*** include-it:cw2 ***)
(**
Let's look at some basic statistics about individual donations:
*)
let alldons = 
  clean |> Array.collect (fun d -> 
    Array.map (snd >> float) d.Donations) |> Series.ofValues

let statsdon = 
  ["Median", alldons |> Stats.median
   "Mean", alldons |> Stats.mean
   "Sum", alldons |> Stats.sum
   "Count", alldons |> Stats.count |> float ] |> series
(*** include-value:statsdon ***)
(**
Is the above chart with total donated amount different for s
maller and larger donations or not? This is what we get 
when we look only at donations of GBP 20 and less:
*)
(*** define-output:cw2a ***)
clean
|> Array.collect (fun d -> d.Donations)
|> Array.filter (fun (_, v) -> v <= 20)
|> Array.filter (fun (d, _) -> d.Year = 2020)
|> Array.groupBy (fun (d, _) -> previousMonday d)
|> Array.map (fun (d, g) -> d, Seq.sumBy snd g)
|> Array.sortBy fst
|> Chart.Column
(*** include-it:cw2a ***)
(**
And this is what we get when we look only at donations larger than GBP 20:
*)
(*** define-output:cw2b ***)
clean
|> Array.collect (fun d -> d.Donations)
|> Array.filter (fun (_, v) -> v > 20)
|> Array.filter (fun (d, _) -> d.Year = 2020)
|> Array.groupBy (fun (d, _) -> previousMonday d)
|> Array.map (fun (d, g) -> d, Seq.sumBy snd g)
|> Array.sortBy fst
|> Chart.Column
(*** include-it:cw2b ***)
(**

# But this does not work. Why?

The issue with this is that fundraisers get removed from the web sites and
we only have first data for May 17, so we cannot really talk about what
happened earlier. How much earlier?

Let's look at this. For all the fundraisers that appeared in one of our earlier 
data scrapes, but then disappeared in one of the later ones, how many days
was there before they were created and then disappeared?
*)
let rowsAt d = 
  let files = 
    [ for s in ["gofundme"; "just"; "virgin"] do
      for k in ["foodbank"; "food-bank"; "soup-kitchen"] do
      yield s, sprintf "../outputs/%s/%s_%s.csv" d s k ]
  (mergeFiles files).Rows.Values

let removed datePairs = 
  [| for od, nd in datePairs do
      let o, n = rowsAt od, rowsAt nd
      let nd = DateTime.Parse nd
      let nks = set [ for r in n -> r.GetAs<string>("Link") ]
      for r in o do
        if r.GetAs "Created" <> "" && not (nks.Contains(r.GetAs<string>("Link"))) then
          let dt = r.GetAs<DateTime> "Created"
          yield r.GetAs<string> "Link", (nd, int (nd - dt).TotalDays / 7) |]
(**
For the fundraisers that disappeared, how old they were (in weeks) when 
they disappeared from our dataset?
*)
(*** define-output:r1 ***)
removed 
  [ "2020-05-17","2020-06-01"; 
    "2020-06-01","2020-06-14"; 
    "2020-06-14","2020-06-29"]
|> Array.map (snd >> snd)
|> Array.countBy id
|> Array.sortBy fst
|> Chart.Column
(*** include-it:r1 ***)
(**
Looking only at those that disappeared after less than 10 weeks:
*)
(*** define-output:r2 ***)
removed 
  [ "2020-05-17","2020-06-01"; 
    "2020-06-01","2020-06-14"; 
    "2020-06-14","2020-06-29"]
|> Array.map (snd >> snd)
|> Array.filter (fun w -> w < 10)
|> Array.countBy id
|> Array.sortBy fst
|> Chart.Column
(*** include-it:r2 ***)
(**
If we look at fundraisers that we have in our data that were created
in March 2020 or later, how many of those disappeared between 
17/5 and 1/6 and between 1/6 and 14/6?
*)
(*** define-output:r3 ***)
let removedKeys = 
  removed 
    [ "2020-05-17","2020-06-01"; 
      "2020-06-01","2020-06-14"; 
      "2020-06-14","2020-06-29"]
  |> Array.map fst |> set

let getRemovedOrNot removed = 
  merged.Rows.Values
  |> Seq.filter (fun r -> r.GetAs "Created" <> "")
  |> Seq.map (fun r -> 
    previousMonday (r.GetAs<DateTime> "Created"),
    removedKeys.Contains (r.GetAs "Link"))
  |> Seq.filter (fun (d, r) -> d > DateTime(2020, 3, 1) && r = removed)
  |> Seq.countBy fst
  |> Seq.sortBy fst

[ getRemovedOrNot true
  getRemovedOrNot false ]
|> Chart.Column
|> Chart.WithLabels ["Removed"; "Not removed"]
(*** include-it:r3 ***)
(**

## Donations to old fundraisers

One thing we can do that makes sense is to look at donations to fundraisers
that were started in 2019 or earlier. We have some 160 of those
and we have full donation data for them, so this is valid:
*)
(*** define-output:old1 ***)
clean
|> Array.filter (fun d -> d.Created.Year < 2020)
|> Array.collect (fun d -> d.Donations)
|> Array.filter (fun (d, _) -> d.Year >= 2019)
|> Array.groupBy (fun (d, _) -> previousMonday d)
|> Array.map (fun (d, g) -> d, Seq.sumBy snd g)
|> Array.sortBy fst
|> Chart.Column
(*** include-it:old1 ***)

(**

# Estimating short-lived from long-lived fundraisers 

What can we do to say something about fundraisers before we started scraping data on 17 May?
One idea is that we can estimate the number of short-term fundraisers (i.e. those that disappeared
before 17 May) from the number of long-term fundraisers (i.e. those that were setup sometime
before 17 May, but are still present in our data set).


Before we do this, we'll do some data cleaning and save `merged.csv` file. This is our 
merged data set generated earlier, now with an extra column `Removed` which indicates
whether a fundraiser has been removed (and if so, when).
*)
let removedSeries = 
  removed 
    [ "2020-05-17","2020-06-01"; 
      "2020-06-01","2020-06-14"; 
      "2020-06-14","2020-06-29"; 
      "2020-06-29","2020-07-12"; 
      ]
  |> Array.map (fun (k, (dt, _)) -> k, dt.ToString("yyyy-MM-dd")) 
  |> series

let withRemoved = merged |> Frame.addCol "Removed" removedSeries
withRemoved.SaveCsv("../outputs/merged.csv")
(**

## Counting short-lived and long-lived fundraisers

A short-lived fundraiser is one that has been removed in less than a specified 
number of weeks. The number of weeks can be set freely - the bigger this is, the
further into the past we will be able to estimate (if we have first data from 
17 May and "long-lived" fundraisers are those that have been in the data set for
at least 4 weeks, then we have enough data to talk about 19 April; if we set this
to 6 weeks, then we can talk about 5 April).

First, let's again see how long it takes for a fundraiser to be removed 
(looking at fundraisers removed in less than 100 days only):
*)
(*** define-output:estsl1 ***)
withRemoved 
|> Frame.mapRows (fun _ row -> 
  try 
    let diff = row.GetAs<DateTime>("Removed") - row.GetAs<DateTime>("Created")
    Some(int diff.TotalDays) 
  with _ -> None)
|> Series.dropMissing
|> Series.mapValues (fun v -> v.Value)
|> Series.values
|> Seq.filter (fun d -> d < 100)
|> Seq.countBy id
|> Seq.sort
|> Chart.Column
(*** include-it:estsl1 ***)
(**

It looks like there is a peak around (roughly) 4 weeks, so we're going to use that as
our separator between short-lived and long-lived now. It is also worth noting that 
no fundraisers get removed in less than 14 days, so we can assume that we have information
on all short-lived fundraisers from 14 days before our first scrape (i.e. 3 May).

Now, let's collect all our long-lived and short-lived fundraisers. For short-lived ones, 
we only look at those created 4 weeks before our last scrape (because we cannot for sure
say which of those will be removed in the next scrape).
*)
let shortDays = 7*4
let shortEnd = DateTime(2020, 6, 1)

let shortLived = 
  [ for row in withRemoved.Rows.Values do
      let removed = row.TryGetAs<DateTime>("Removed")
      if row.GetAs<string>("Created") <> "" then
        let created = row.GetAs<DateTime>("Created")
        if created < shortEnd && removed.HasValue then
          let days = int (removed.Value - created).TotalDays
          if days < shortDays then
            yield previousMonday created, days ]

let longLived = 
  [ for row in withRemoved.Rows.Values do
      if row.GetAs<string>("Created") <> "" then
        let created = row.GetAs<DateTime>("Created")
        let removed = 
          row.TryGetAs<DateTime>("Removed") 
          |> OptionalValue.asOption |> Option.map (fun dt ->
          int (dt - created).TotalDays)
        if created < shortEnd && (removed.IsNone || removed.Value > shortDays) then
          yield previousMonday created, removed ]
(**
Now we have two things:

 - `shortLived` is a list of fundraisers created before `shortEnd` that were removed in 
    less than `shortDays` number of days. The list contains the date of the Monday of
    the week in which they were created together with the number of days they stayed
    in our dataset.

 - `longLived` is a list of fundraisers that were created before `shortEnd` that 
    were either never removed or stayed in our dataset at least `shortDays` number 
    of days. As before, the list contains Monday of a week together with (optional)
    number of days.

Once again, the number of short-lived fundraisers that stayed a given number of days
and a number of long-lived fundraisers that stayed a given number of days (before
being removed):
*)          
(*** define-output:estsl2 ***)
shortLived
|> Seq.countBy snd
|> Seq.sortBy fst
|> Chart.Column
(*** include-it:estsl2 ***)
(*** define-output:estsl3 ***)
longLived
|> Seq.countBy snd
|> Seq.choose (fun (k, v) -> if k.IsNone then None else Some(k.Value, v))
|> Seq.sortBy fst
|> Chart.Column
(*** include-it:estsl3 ***)
(**
## Visualizing number of short and long-lived fundraisers

Let's now look at the data that we have about short-term and long-term 
fundraisers. The following shows the number of short-term fundraisers started
on the weeks for which we have enough data to be able to say this:
*)
(*** define-output:estsl4 ***)
let sc = shortLived |> Seq.countBy fst |> Seq.sortBy fst
sc |> Chart.Column
(*** include-it:estsl4 ***)
(**
And the following shows the number of long-term fundraisers, started on the
weeks for which we have enough data:
*)
(*** define-output:estsl5 ***)
let lc = 
  longLived 
  |> Seq.filter (fun (dt, _) -> dt.Year = 2020)
  |> Seq.filter (fun (dt, _) -> (dt.Month = 4 && dt.Day >= 19) || dt.Month = 5)
  |> Seq.countBy fst |> Seq.sortBy fst
lc |> Chart.Column
(*** include-it:estsl5 ***)
(**
In order to estimate the number of short-lived fundraisers from the number
of long-lived funraisers, we're going to assume that the ratio between them
(started each week) is roughly the same. Let's compare the two first:
*)
(*** define-output:estsl6 ***)
Chart.Column([lc;sc])
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["Long lived"; "Short lived"]
(*** include-it:estsl6 ***)
(**
To see this better, we can look at a stacked bar chart where the values
add up to 100%:
*)
(*** define-output:estsl7 ***)
let sc4, lc4 = sc, Seq.skip 2 lc
Chart.Column([ 
    [ for (d, s), (_, l) in Seq.zip sc4 lc4 -> d, float l / float (s + l) * 100. ]
    [ for (d, s), (_, l) in Seq.zip sc4 lc4 -> d, float s / float (s + l) * 100. ] ])
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["Long lived"; "Short lived"]
(*** include-it:estsl7 ***)
(**
For the estimation, we'll just calculate the average ratio between short-lived
and long-lived fundraisers:
*)
let ratio = 
  Seq.map2 (fun (_, s) (_, l) -> float s / float l) sc4 lc4 
  |> Seq.average
(**
How far back can we reasonably use this estimation? Our first data scrape is
on May 17. We assume that long-term fundraisers are those that have been in 
our data set for at least 4 weeks, so our data set contains all long-term 
fundraisers started on April 19. This is our `lc` value defined earlier.

We can thus reasonably assume that we know the number of short-term fundraisers
from May 4 onward (because all fundraisers stay at least 14 days) and we can 
estimate the number of short-term from long-term from April 20 onward:
*)
(*** define-output:estsl8 ***)
let scd = sc |> Seq.map fst |> set
let est = seq { 
  for d, v in lc do 
    if not(scd.Contains(d)) then 
      yield d, int (float v * ratio) }

[ lc; sc; est ] 
|> Chart.Column 
|> Chart.WithOptions(Options(isStacked=true)) 
|> Chart.WithLabels ["Long lived"; "Short lived"; "Short lived estimate" ]
(*** include-it:estsl8 ***)
(**
In summary, if we add the two kinds of fundraisers, we have the following total numbers:
*)
(*** define-output:estsl9 ***)
Seq.zip lc (Seq.append est sc)
|> Seq.map (fun ((ld, l), (sd, s)) ->
  if ld <> sd then failwith "Wrong date"
  ld, l + s)
|> Chart.Column 
(*** include-it:estsl9 ***)
