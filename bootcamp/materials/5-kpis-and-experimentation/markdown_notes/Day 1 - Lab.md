# Day 1 - Lab

First of all, to setup, open an account on Statsig, get a free Server API key from the settings, and add it to your env variables.

Then, install the dependencies for this module (kpis and experimentation), and finally run the flask server with `python src/server.py`, and visit `localhost:5000` to make sure everything is working properly.

If you now go to `/tasks`, you’ll see you have received a specific color.

Now open [statsig console](https://console.statsig.com), click “experiments” on the left, and click “Get started”. Call the experiment `button_color_v3` and in the hypothesis write “I think the red button is the best one”, then click “create”.

Now you want to think about the metrics, let’s pick “dau” (daily active users).

Then, at the bottom of the page, under “Groups and Parameters”, click “Add a parameter”, call it “Button Color”. Then put “Blue” in control and “Red” in test. You can add a couple more groups and give them value “Green” and “Orange”, respectively. No we have 4 groups all evenly split (25% each).

When you’re done, click “Save”, and at the top of the page click “Start” then “Start” again in the modal that appears.

If you now go back to `localhost:5000/tasks` and add this query `?random=true`, and refresh the page a few times, you’ll see the color changes each time. This is to simulate different users.

Now what we just did, is we’re simulating different daily active users, so we will know if someone is daily active. But now we want to know “ok, do they do more, after visiting a page?”

If you look at the bottom of the `tasks` page, see there’s a little link to “signup”, which you can click.

In the code, under the `signup()` function, you will see that we’re calling `statig.log_event()`. This is to communicate to Statsig that the user visited the signup page.

Now play with these links a bit to generate some data, then go back to statsig → experiments → `button_color_v3` → Diagnostics, and you will see a bunch of events we created.

Unfortunately, it can take a while, even a day, for Statsig to generate some reports, so this lab ain’t exactly comprehensive.

One thing that you might want to keep in mind is “when is stuff logged”. Right now, our events are logged server-side, but you can also do client-side logging, and they have different benefits and risks.

Server side logging is easier to set up, whereas client side logging is trickier. One of the reasons is that we don’t want random clients to just log whatever they want to our servers, so you have to deal with OAuth, to authorize these requests.

On the flip side, you get better fidelity data, because you can track when the user clicks the action, rather than when the server picked it up. So you get better, higher quality events, as well as more variety, like scroll time, view time etc…

In Statsig there are other fancy functionalities that you can control, for instance you have “Feature gates”, that are like toggles, e.g. enable or disable a feature to see how the user behaves.

Another thing that you might find in Statsig, is that you can split your experiments in groups, and notice that something that is statistically significant for one group, might not be significant for another.

This is important to consider, because overall you might see (or not) an effect, but if you break it down to smaller groups, you might notice something completely different.

One useful feature in Statsig is that you can tag your metrics. You can create new tags under settings, and then you can assign them to metrics however you please. This way, you could for instance tag certain metrics as **guardrail** (we seen in previous lectures that guardrail metrics are those metrics that if they behave a certain way, e.g. if they go down, they block the new feature from being released).