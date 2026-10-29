import os
import discord
import env  # loads .env

client = discord.Client()

@client.event
async def on_ready():
    print(f"✅ Logged in as {client.user}")
    print(f"✅ Listening to all channels...")

@client.event
async def on_message(message):
    print(f"📨 Message from {message.channel.name}: {message.content[:50]}...")

client.run(os.environ["DISCORD_TOKEN"])